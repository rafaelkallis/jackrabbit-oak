package com.rafaelkallis.shared;

import static com.rafaelkallis.shared.TraverseUtils.LevelOrder;
import static com.rafaelkallis.shared.TraverseUtils.PreOrder;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.PROPERTY_NAMES;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    static final Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static void setUpCompleteTree(
            final Tree tree,
            final int fanout,
            final int maxDepth
    ) {
        LOG.debug(String.format("create complete tree with root: %s, fanout: %s,depth: %s", tree.getPath(), fanout, maxDepth));
        final int rootDepth = PathUtils.getDepth(tree.getPath());
        PreOrder(tree, (node) -> {
            final int nodeDepth = PathUtils.getDepth(node.getPath()) - rootDepth;
            if (nodeDepth < maxDepth - 1) {
                for (char i = 'a'; i < 'a' + fanout; i++) {
                    final String childName = Character.toString(i);
                    if (!node.hasChild(childName)) {
                        node.addChild(childName);
                    }
                }
            }
        });
    }

    public static String[] slice(
            final Tree tree,
            final int minDepth,
            final int maxDepth
    ) {
        List<String> list = new ArrayList<>();
        final int rootDepth = PathUtils.getDepth(tree.getPath());
        for (Tree node : LevelOrder(tree)){
            final int nodeDepth = PathUtils.getDepth(node.getPath()) - rootDepth;
            if (minDepth <= nodeDepth && nodeDepth < maxDepth) {
                list.add(node.getPath());
            }
        }
        // LevelOrder(tree, (node) -> {
        //     final int nodeDepth = PathUtils.getDepth(node.getPath()) - rootDepth;
        //     if (minDepth <= nodeDepth && nodeDepth < maxDepth) {
        //         list.add(node.getPath());
        //     }
        // });
        return list.toArray(new String[list.size()]);
    }

    public static Consumer.One<String> toggleProperty(ClusterNode clusterNode) {
        return (path) -> {
            try {
                clusterNode.transaction(root -> {
                        Tree node = root.getTree(path);
                        if (node.hasProperty("pub")) {
                            node.removeProperty("pub");
                        } else {
                            node.setProperty("pub", "now");
                        }
                    }).commit();
            } catch (CommitFailedException e) {
                LOG.error("commit failed", e);
            }
        };
    }

    public static Consumer.One<String> toggleTwice(ClusterNode clusterNode) {
        return (path) -> {
            try {
                clusterNode.transaction(root -> {
                        root.getTree(path).setProperty("pub", "now");
                    }).commit();
                clusterNode.transaction(root -> {
                        root.getTree(path).removeProperty("pub");
                    }).commit();
            } catch (CommitFailedException e) {
                LOG.error("commit failed", e);
            } catch (IllegalStateException e) {
                LOG.error("illegal state exception: {}", path);
            }
        };
    }

    public static void initializePropertyIndex(final Root root, final String propName) {
        Tree index = root.getTree(PathUtils.concat("/", INDEX_DEFINITIONS_NAME));
        if (!index.hasChild(propName)) {
            Tree prop = index.addChild(propName);
            prop.setProperty(TYPE_PROPERTY_NAME, "property");
            prop.setProperty("jcr:primaryType", INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
            prop.setProperty(PROPERTY_NAMES, propName, Type.NAME);
            prop.setProperty(UNIQUE_PROPERTY_NAME, false);
            prop.setProperty(REINDEX_PROPERTY_NAME, true);
        }
    }

    public static int firstNode(final int minDepth) {
        return (1 << minDepth) - 1;
    }

    public static int firstNode(final int minDepth, final int fanout) {
        return (int) ((Math.pow(fanout, minDepth) - 1) / (fanout - 1));
    }

    public static int lastNode(final int maxDepth) {
        return firstNode(maxDepth) - 1;
    }

    public static int lastNode(final int maxDepth, final int fanout) {
        return firstNode(maxDepth, fanout) - 1;
    }

    public static int totalNodes(final int minDepth, final int maxDepth) {
        return lastNode(maxDepth) - firstNode(minDepth) + 1;
    }

    public static int totalNodes(final int minDepth, final int maxDepth, final int fanout) {
        return lastNode(maxDepth, fanout) - firstNode(minDepth, fanout) + 1;
    }

    public static String mapToPath(final int k) {
        final String kBase = Integer.toString(k + 1, 2); // 1010
        final String kTrunc = kBase.substring(1); // 010

        StringBuilder path = new StringBuilder();
        for (char c : kTrunc.toCharArray()) {
            final int i = Integer.parseInt(Character.toString(c), 2);
            path.append((char) (i + 'a')).append("/");
        }
        return path.toString();
    }

    public static DocumentNodeState getNode(NodeStore nodeStore, String absPath) {
        NodeState targetNode = nodeStore.getRoot();
        for (String child : PathUtils.elements(absPath)) {
            targetNode = targetNode.getChildNode(child);
        }
        return (DocumentNodeState) targetNode;
    }

    public static Iterable<NodeState> childNodes(final NodeState node) {
        Iterable<String> childNodeNameIterable = node.getChildNodeNames();
        return () -> {
            Iterator<String> childNodeNameIterator = childNodeNameIterable.iterator();
            return new Iterator<NodeState>() {
                @Override
                public boolean hasNext() {
                    return childNodeNameIterator.hasNext();
                }

                @Override
                public NodeState next() {
                    return node.getChildNode(childNodeNameIterator.next());
                }
            };
        };
    }

    public static Iterable<Tree> childNodes(final Tree node) {
        return node.getChildren();
    }

    public static <R> Supplier<R> memoize(Supplier<R> supplier) {
        AtomicReference<R> v = new AtomicReference<>();
        return () -> {
            if (v.get() == null) {
                v.set(supplier.get());
            }
            return v.get();
        };
    }

    public static int linearProber(
            final int _i,
            final int clusterNode,
            final int nClusterNodes,
            final int bins
    ) {
        int i = _i;
        while (i % nClusterNodes != clusterNode) {
            i++;
        }
        return i < bins ? i : clusterNode;
    }

    public static <R> Supplier<R> throwException(String message) {
        return () -> {
            throw new RuntimeException(message);
        };
    }

    public static Supplier<Long> millisSinceNow() {
        return millisSince(System.currentTimeMillis());
    }

    public static Supplier<Long> millisSince(final long start) {
        return () -> System.currentTimeMillis() - start;
    }

    public static Tree generatePath(final Tree root, String path){
        Tree r = root;
        for(String label: PathUtils.elements(path)){
            if (r.hasChild(label)){
                r = r.getChild(label);
            } else {
                r = r.addChild(label);
            }
        }
        return r;
    }

    public static boolean conjuct(Iterable<Boolean> iterable) {
        boolean a = true;
        for (boolean b: iterable) {
            a &= b;
        }
        return a;
    }

    public static boolean isMatching(Tree node) {
        return node.hasProperty("match") &&
            node.getProperty("match").getValue(Type.BOOLEAN);
    }

    public static boolean isVolatile(Tree node, DocumentNodeStore store) {
        int vol = 0;
        String key = PathUtils.getDepth(node.getPath()) + ":" + node.getPath();
        NodeDocument document = store.getDocumentStore().find(Collection.NODES, key);
        for (Revision r : document.getLocalDeleted().keySet()) {
            if (!isInSlidingWindow(r, store)) {
                break;
            }
            if (!isVisible(r, store)) {
                continue;
            }
            if (vol++ >= store.getVolatilityThreshold()) {
                break;
            }
        }
        return vol >= store.getVolatilityThreshold();
    }

    private static boolean isVisible(Revision r, DocumentNodeStore store) {
        int clusterId = store.getClusterId();
        return r.getClusterId() == clusterId
            || (r.compareRevisionTimeThenClusterId(store.getHeadRevision().getRevision(clusterId)) < 0);
    }

    private static boolean isInSlidingWindow(Revision r, DocumentNodeStore store) {
        return System.currentTimeMillis() - store.getSlidingWindowLength() < r.getTimestamp();
    }

    // public static Function<String, Set<String>> nativeQuery(
    //                                                         final ClusterNode o,
    //                                                         final BiConsumer<Long, Long> hook
    //                                                         ) {
    //     return (String path) -> {
    //         final Supplier<Long> delta = millisSinceNow();
    //         final Set<String> resultSet = new HashSet<>();
    //         try {
    //             o.transaction(root -> {
    //                     try {
    //                         for (ResultRow resultRow : root.getQueryEngine().executeQuery(
    //                                                                                       path + "/*[@pub='now']",
    //                                                                                       Query.XPATH,
    //                                                                                       Collections.emptyMap(),
    //                                                                                       Collections.emptyMap()
    //                                                                                       ).getRows()) {
    //                             resultSet.add(resultRow.getPath());
    //                         }
    //                     } catch (ParseException e) {
    //                         LOG.error("parse exception", e);
    //                     }
    //                 }).commit();
    //         } catch (CommitFailedException e) {
    //             LOG.error("commit failed", e);
    //         }
    //         hook.accept(delta.get(), null);
    //         return resultSet;
    //     };
    // }
}
