/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import com.rafaelkallis.WAPI;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.index.counter.ApproximateCounter;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditor;
import org.apache.jackrabbit.oak.plugins.index.counter.jmx.NodeCounter;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.query.FilterIterators;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.QueryLimits;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Deque;
import java.util.Iterator;
import java.util.Set;

import static com.google.common.collect.Queues.newArrayDeque;
import static com.rafaelkallis.WAPI.getDocumentFromAbsPath;
import static com.rafaelkallis.WAPI.isVolatile;
import static com.rafaelkallis.WAPI.isWorkloadAware;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;

/**
 * An IndexStoreStrategy implementation that saves the nodes under a hierarchy
 * that mirrors the repository tree. <br>
 * This should minimize the chance that concurrent updates overlap on the same
 * content node.<br>
 * <br>
 * For example for a node that is under {@code /test/node}, the index
 * structure will be {@code /oak:index/index/test/node}:
 * <p>
 * <pre>
 * {@code
 * /
 *   test
 *     node
 *   oak:index
 *     index
 *       test
 *         node
 * }
 * </pre>
 */
public class ContentMirrorStoreStrategy implements IndexStoreStrategy {

    static final Logger LOG = LoggerFactory.getLogger(ContentMirrorStoreStrategy.class);

    /**
     * logging a warning every {@code oak.traversing.warn} traversed nodes. Default {@code 10000}
     */
    public static final int TRAVERSING_WARN = Integer.getInteger("oak.traversing.warn", 10000);

    private final String indexName;

    public ContentMirrorStoreStrategy() {
        this(INDEX_CONTENT_NODE_NAME);
    }

    public ContentMirrorStoreStrategy(String indexName) {
        this.indexName = indexName;
    }

    /**
     * @param index      the index node supplier, NodeBuilder at "/oak:index/pub/:index/"
     * @param path       path stored in the index, "/a/b"
     * @param indexName  the name of the index. May be null. "pub"
     * @param indexMeta  the definition of the index. May be null. NodeBuilder at "/oak:index/pub/"
     * @param beforeKeys keys that no longer apply to the path, [ "now" ]
     * @param afterKeys  keys that now do apply to the path, [ "now" ]
     */
    @Override
    public void update(
            Supplier<NodeBuilder> index,
            String path,
            @Nullable final String indexName,
            @Nullable final NodeBuilder indexMeta,
            Set<String> beforeKeys, Set<String> afterKeys) {
        for (String key : beforeKeys) {
            remove(index.get(), indexName, key, path);
        }
        for (String key : afterKeys) {
            insert(index.get(), key, path);
        }
    }

    private void remove(
            final NodeBuilder index,
            final @Nullable String indexName,
            final String key,
            final String path
    ) {
        ApproximateCounter.adjustCountSync(index, -1);
        NodeBuilder builder = index.getChildNode(key);
        if (builder.exists()) {
            ApproximateCounter.adjustCountSync(builder, -1);
            // Collect all builders and their absolute path along the given path
            Deque<NodeBuilderPath> builders = newArrayDeque();
            builders.addFirst(new NodeBuilderPath(
                    builder,
                    PathUtils.concat("/" + INDEX_DEFINITIONS_NAME, indexName, key)
            ));

            // Descend to the correct location in the index tree
            for (String name : PathUtils.elements(path)) {
                NodeBuilderPath parent = builders.peekFirst();
                builders.addFirst(new NodeBuilderPath(
                        parent.nodeBuilder.getChildNode(name),
                        PathUtils.concat(parent.path, name)
                ));
            }

            // Drop the match value, if present
            NodeBuilder target = builders.peekFirst().nodeBuilder;
            if (target.exists()) {
                target.removeProperty("match");
            }

            // Prune all index nodes that are no longer needed
            prune(builders);
        }
    }

    private void insert(NodeBuilder index, String key, String value) {
        ApproximateCounter.adjustCountSync(index, 1);
        // NodeBuilder builder = index.child(key);
        NodeBuilder builder = fetchKeyNode(index, key);
        ApproximateCounter.adjustCountSync(builder, 1);
        for (String name : PathUtils.elements(value)) {
            builder = builder.child(name);
        }
        builder.setProperty("match", true);
    }

    public Iterable<String> query(final Filter filter, final String indexName,
                                  final NodeState indexMeta, final String indexStorageNodeName,
                                  final Iterable<String> values) {
        final NodeState index = indexMeta.getChildNode(indexStorageNodeName);
        return new Iterable<String>() {
            @Override
            public Iterator<String> iterator() {
                PathIterator it = new PathIterator(filter, indexName, "");
                if (values == null) {
                    it.setPathContainsValue(true);
                    it.enqueue(getChildNodeEntries(index).iterator());
                } else {
                    for (String p : values) {
                        NodeState property = index.getChildNode(p);
                        if (property.exists()) {
                            // we have an entry for this value, so use it
                            it.enqueue(Iterators.singletonIterator(
                                    new MemoryChildNodeEntry("", property)));
                        }
                    }
                }
                return it;
            }
        };
    }

    @Nonnull
    Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull final NodeState index) {
        return index.getChildNodeEntries();
    }

    @Override
    public Iterable<String> query(final Filter filter, final String name,
                                  final NodeState indexMeta, final Iterable<String> values) {
        return query(filter, name, indexMeta, this.indexName, values);
    }

    @Override
    public long count(NodeState root, NodeState indexMeta, Set<String> values, int max) {
        return count(null, root, indexMeta, this.indexName, values, max);
    }

    @Override
    public long count(final Filter filter, NodeState root, NodeState indexMeta, Set<String> values, int max) {
        return count(filter, root, indexMeta, this.indexName, values, max);
    }

    long count(Filter filter, NodeState root, NodeState indexMeta, final String indexStorageNodeName,
               Set<String> values, int max) {
        NodeState index = indexMeta.getChildNode(indexStorageNodeName);
        long count = -1;
        if (values == null) {
            // property is not null
            PropertyState ec = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
            if (ec != null) {
                // negative value implies fall-back to counting
                count = ec.getValue(Type.LONG);
            } else {
                // negative value means that approximation isn't available
                count = ApproximateCounter.getCountSync(index);
            }
            if (count < 0) {
                CountingNodeVisitor v = new CountingNodeVisitor(max);
                v.visit(index);
                count = v.getEstimatedCount();
                if (count >= max) {
                    // "is not null" queries typically read more data
                    count *= 10;
                }
            }
        } else {
            // property = x, or property in (x, y, z)
            int size = values.size();
            if (size == 0) {
                return 0;
            }
            PropertyState ec = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
            if (ec != null) {
                count = ec.getValue(Type.LONG);
                if (count >= 0) {
                    // assume 10*NodeCounterEditor.DEFAULT_RESOLUTION entries per key, so that this index is used
                    // instead of traversal, but not instead of a regular property index
                    long keyCount = count / (10 * NodeCounterEditor.DEFAULT_RESOLUTION);
                    ec = indexMeta.getProperty(KEY_COUNT_PROPERTY_NAME);
                    if (ec != null) {
                        keyCount = ec.getValue(Type.LONG);
                    }
                    // cast to double to avoid overflow 
                    // (entryCount could be Long.MAX_VALUE)
                    // the cost is not multiplied by the size, 
                    // otherwise the traversing index might be used              
                    keyCount = Math.max(1, keyCount);
                    count = (long) ((double) count / keyCount) + size;
                }
            } else {
                // for this index, property "entryCount" is not set
                long approxMax = 0;
                long approxCount = ApproximateCounter.getCountSync(index);
                if (approxCount != -1) {
                    // approximate count is available for the index:
                    // check approximate counts for each value
                    for (String p : values) {
                        NodeState s = index.getChildNode(p);
                        if (s.exists()) {
                            long a = ApproximateCounter.getCountSync(s);
                            if (a != -1) {
                                approxMax += a;
                            } else if (approxMax > 0) {
                                // in absence of approx count for a key we should be conservative
                                approxMax += 10 * NodeCounterEditor.DEFAULT_RESOLUTION;
                            }
                        }
                    }
                    if (approxMax > 0) {
                        count = approxMax;
                    }
                }
            }
            // still, property = x, or property in (x, y, z),
            // and we don't know the count ("entryCount" = -1)
            if (count < 0) {
                count = 0;
                max = Math.max(10, max / size);
                int i = 0;
                for (String p : values) {
                    if (count > max && i > 3) {
                        // the total count is extrapolated from the the number
                        // of values counted so far to the total number of values
                        count = count * size / i;
                        break;
                    }
                    NodeState s = index.getChildNode(p);
                    if (s.exists()) {
                        CountingNodeVisitor v = new CountingNodeVisitor(max);
                        v.visit(s);
                        count += v.getEstimatedCount();
                    }
                    i++;
                }
            }
        }

        String filterRootPath = null;
        if (filter != null &&
                filter.getPathRestriction().equals(Filter.PathRestriction.ALL_CHILDREN)) {
            filterRootPath = filter.getPath();
        }

        if (filterRootPath != null) {
            // scale cost according to path restriction
            long totalNodesCount = NodeCounter.getEstimatedNodeCount(root, "/", true);
            if (totalNodesCount != -1) {
                long filterPathCount = NodeCounter.getEstimatedNodeCount(root, filterRootPath, true);
                if (filterPathCount != -1) {
                    // assume nodes in the index are evenly distributed in the repository (old idea)
                    long countScaledDown = (long) ((double) count / totalNodesCount * filterPathCount);
                    // assume 80% of the indexed nodes are in this subtree
                    long mostNodesFromThisSubtree = (long) (filterPathCount * 0.8);
                    // count can at most be the assumed subtree size
                    count = Math.min(count, mostNodesFromThisSubtree);
                    // this in theory should not have any effect, 
                    // except if the above estimates are incorrect,
                    // so this is just for safety feature
                    count = Math.max(count, countScaledDown);
                }
            }
        }
        return count;
    }

    /**
     * An iterator over paths within an index node.
     */
    static class PathIterator implements Iterator<String> {

        private final Filter filter;
        private final String indexName;
        private final Deque<Iterator<? extends ChildNodeEntry>> nodeIterators =
                Queues.newArrayDeque();
        private int readCount;
        private int intermediateNodeReadCount;
        private boolean init;
        private boolean closed;
        private String filterPath;
        private String pathPrefix;
        private String parentPath;
        private String currentPath;
        private boolean pathContainsValue;

        /**
         * Keep the returned path, to avoid returning duplicate entries.
         */
        private final Set<String> knownPaths = Sets.newHashSet();
        private final QueryLimits settings;

        PathIterator(Filter filter, String indexName, String pathPrefix) {
            this.filter = filter;
            this.pathPrefix = pathPrefix;
            this.indexName = indexName;
            boolean shouldDescendDirectly = filter.getPathRestriction().equals(Filter.PathRestriction.ALL_CHILDREN);
            if (shouldDescendDirectly) {
                filterPath = filter.getPath();
                if (PathUtils.denotesRoot(filterPath)) {
                    filterPath = "";
                }
            } else {
                filterPath = "";
            }
            parentPath = "";
            currentPath = "/";
            this.settings = filter.getQueryLimits();
        }

        void enqueue(Iterator<? extends ChildNodeEntry> it) {
            nodeIterators.addLast(it);
        }

        void setPathContainsValue(boolean pathContainsValue) {
            if (init) {
                throw new IllegalStateException("This iterator is already initialized");
            }
            this.pathContainsValue = pathContainsValue;
        }

        @Override
        public boolean hasNext() {
            if (!closed && !init) {
                fetchNext();
                init = true;
            }
            return !closed;
        }

        private void fetchNext() {
            while (true) {
                fetchNextPossiblyDuplicate();
                if (closed) {
                    return;
                }
                if (pathContainsValue) {
                    String value = PathUtils.elements(currentPath).iterator().next();
                    currentPath = PathUtils.relativize(value, currentPath);
                    // don't return duplicate paths:
                    // Set.add returns true if the entry was new,
                    // so if it returns false, it was already known
                    if (!knownPaths.add(currentPath)) {
                        continue;
                    }
                }
                break;
            }
        }

        private void fetchNextPossiblyDuplicate() {
            while (!nodeIterators.isEmpty()) {
                Iterator<? extends ChildNodeEntry> iterator = nodeIterators.getLast();
                if (iterator.hasNext()) {
                    ChildNodeEntry entry = iterator.next();

                    NodeState node = entry.getNodeState();

                    String name = entry.getName();
                    if (NodeStateUtils.isHidden(name)) {
                        continue;
                    }
                    currentPath = PathUtils.concat(parentPath, name);

                    if (!"".equals(filterPath)) {
                        String p = currentPath;
                        if (pathContainsValue) {
                            String value = PathUtils.elements(p).iterator().next();
                            p = PathUtils.relativize(value, p);
                        }
                        if ("".equals(pathPrefix)) {
                            p = PathUtils.concat("/", p);
                        } else {
                            p = PathUtils.concat(pathPrefix, p);
                        }
                        if (!"".equals(p) &&
                                !p.equals(filterPath) &&
                                !PathUtils.isAncestor(p, filterPath) &&
                                !PathUtils.isAncestor(filterPath, p)) {
                            continue;
                        }
                    }

                    nodeIterators.addLast(node.getChildNodeEntries().iterator());
                    parentPath = currentPath;

                    if (node.getBoolean("match")) {
                        readCount++;
                        if (readCount % TRAVERSING_WARN == 0) {
                            FilterIterators.checkReadLimit(readCount, settings);
                            LOG.warn("Index-Traversed {} nodes ({} index entries) using index {} with filter {}", readCount, intermediateNodeReadCount, indexName, filter);
                        }
                        return;
                    } else {
                        intermediateNodeReadCount++;
                    }

                } else {
                    nodeIterators.removeLast();
                    parentPath = PathUtils.getParentPath(parentPath);
                }
            }
            currentPath = null;
            closed = true;
        }

        @Override
        public String next() {
            if (closed) {
                throw new IllegalStateException("This iterator is closed");
            }
            if (!init) {
                fetchNext();
                init = true;
            }
            String result = PathUtils.concat(pathPrefix, currentPath);
            fetchNext();
            return result;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

    }

    /**
     * A node visitor to recursively traverse a number of nodes.
     */
    interface NodeVisitor {
        void visit(NodeState state);
    }

    /**
     * A node visitor that counts the number of matching nodes up to a given
     * maximum, in order to estimate the number of matches.
     */
    static class CountingNodeVisitor implements NodeVisitor {

        /**
         * The maximum number of matching nodes to count.
         */
        final int maxCount;

        /**
         * The current count of matching nodes.
         */
        int count;

        /**
         * The current depth (number of parent nodes).
         */
        int depth;

        /**
         * The sum of the depth of all matching nodes. This value is used to
         * calculate the average depth.
         */
        long depthTotal;

        CountingNodeVisitor(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void visit(NodeState state) {
            if (state.hasProperty("match")) {
                count++;
                depthTotal += depth;
            }
            if (count < maxCount) {
                depth++;
                for (ChildNodeEntry entry : state.getChildNodeEntries()) {
                    if (count >= maxCount) {
                        break;
                    }
                    visit(entry.getNodeState());
                }
                depth--;
            }
        }

        /**
         * The number of matches (at most the maximum count).
         *
         * @return the match count
         */
        int getCount() {
            return count;
        }

        /**
         * The number of estimated matches. This value might be higher than the
         * number of counted matches, if the maximum number of matches has been
         * reached. It is based on the average depth of matches, and the average
         * number of child nodes.
         *
         * @return the estimated matches
         */
        int getEstimatedCount() {
            if (count < maxCount) {
                return count;
            }
            double averageDepth = (int) (depthTotal / count);
            // the number of estimated matches is higher
            // the higher the average depth of the first hits
            long estimatedNodes = (long) (count * Math.pow(1.1, averageDepth));
            estimatedNodes = Math.min(estimatedNodes, Integer.MAX_VALUE);
            return Math.max(count, (int) estimatedNodes);
        }

    }

    /**
     * fetch from the index the <i>key</i> node
     *
     * @param index the current index root
     * @param key   the 'key' to fetch from the repo
     * @return the node representing the key
     */
    NodeBuilder fetchKeyNode(@Nonnull NodeBuilder index,
                             @Nonnull String key) {
        return index.child(key);
    }

    /**
     * Physically prune a list of nodes from the index
     *
     * @param builders list of nodes to prune
     */
    private void prune(final Deque<NodeBuilderPath> builders) {
        for (final NodeBuilderPath node : builders) {
            if (node.nodeBuilder.getBoolean("match")) {
                return;
            }
            if (node.nodeBuilder.getChildNodeCount(1) > 0) {
                return;
            }
            if (!node.nodeBuilder.exists()) {
                return;
            }
            if (isWorkloadAware() && isVolatile(getDocumentFromAbsPath(node.path))) {
                return;
            }
            node.nodeBuilder.remove();
        }
    }



    @Override
    public boolean exists(Supplier<NodeBuilder> index, String key) {
        // This is currently not implemented, because there is no test case for it,
        // and because there is currently no need for this method with this class.
        // We would need to traverse the tree and search for an entry "match".
        // See also OAK-2663 for a potential (but untested) implementation.
        throw new UnsupportedOperationException();
    }

    @Override
    public String getIndexNodeName() {
        return indexName;
    }



    private static class NodeBuilderPath {
        private NodeBuilder nodeBuilder;
        private String path;

        public NodeBuilderPath(NodeBuilder nodeBuilder, String path) {
            this.nodeBuilder = nodeBuilder;
            this.path = path;
        }
    }
}