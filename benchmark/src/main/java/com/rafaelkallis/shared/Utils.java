package com.rafaelkallis.shared;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.rafaelkallis.shared.TraverseUtils.PostOrder;
import static com.rafaelkallis.shared.TraverseUtils.PreOrder;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.*;

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
        PreOrder(tree, (node) -> {
            final int nodeDepth = PathUtils.getDepth(node.getPath()) - rootDepth;
            if (minDepth <= nodeDepth && nodeDepth < maxDepth) {
                list.add(node.getPath());
            }
        });
        return list.toArray(new String[list.size()]);
    }

    public static void toggleProperty(final Tree node) {
        if (node.hasProperty("pub")) {
            node.removeProperty("pub");
        } else {
            node.setProperty("pub", "now");
        }
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

    public static DocumentNodeState getNode(DocumentNodeStore nodeStore, String absPath) {
        DocumentNodeState targetNode = nodeStore.getRoot();
        for (String child : PathUtils.elements(absPath)) {
            targetNode = (DocumentNodeState) targetNode.getChildNode(child);
        }
        return targetNode;
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
}
