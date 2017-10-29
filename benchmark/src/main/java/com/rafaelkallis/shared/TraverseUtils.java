package com.rafaelkallis.shared;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.util.Iterator;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.rafaelkallis.shared.Utils.childNodes;
import static com.rafaelkallis.shared.Utils.getNode;

public class TraverseUtils {

    /**
     * Applies the given function to ach node in pre-order traversal
     *
     * @param root
     * @param func
     */
    public static void LevelOrder(
            final Tree root,
            final Consumer<Tree> func
    ) {
        Iterator<Tree> levelOrder = LevelOrder(root);
        while (levelOrder.hasNext()) {
            func.accept(levelOrder.next());
        }
    }

    public static Iterator<Tree> LevelOrder(final Tree root) {
        final MutableIterator<Tree> nodes = new MutableIterator<>(root);
        return new Iterator<Tree>() {
            @Override
            public boolean hasNext() {
                return nodes.hasNext();
            }

            @Override
            public Tree next() {
                final Tree next = nodes.next();
                nodes.append(next.getChildren().iterator());
                return next;
            }
        };
    }

    public static Iterator<NodeState> LevelOrder(final NodeState root) {
        final MutableIterator<NodeState> nodes = new MutableIterator<>(root);
        return new Iterator<NodeState>() {
            @Override
            public boolean hasNext() {
                return nodes.hasNext();
            }

            @Override
            public NodeState next() {
                final NodeState next = nodes.next();
                final Iterator<String> childNodeNames = next.getChildNodeNames().iterator();
                nodes.append(new Iterator<NodeState>() {
                    @Override
                    public boolean hasNext() {
                        return childNodeNames.hasNext();
                    }

                    @Override
                    public NodeState next() {
                        return next.getChildNode(childNodeNames.next());
                    }
                });
                return next;
            }
        };
    }

    /**
     * Applies the given function to each node in pre-order traversal
     *
     * @param node
     * @param func
     */
    public static void PreOrder(
            final Tree node,
            final Consumer<Tree> func
    ) {
        func.accept(node);
        for (Tree child : node.getChildren()) {
            PreOrder(child, func);
        }
    }

    /**
     * Applies the given function to each node in post-order traversal
     *
     * @param node
     * @param func
     */
    public static void PostOrder(
            final Tree node,
            final Consumer<Tree> func
    ) {
        for (Tree child : childNodes(node)) {
            PostOrder(child, func);
        }
        func.accept(node);
    }

    public static <R> R Accumulate(
            final DocumentNodeStore documentNodeStore,
            final String absPath,
            final TriFunction<DocumentNodeState, String, Iterable<R>, R> func
    ) {
        return Accumulate(
                getNode(documentNodeStore, absPath),
                absPath,
                func
        );
    }

    public static void PreOrder(
            final DocumentNodeStore nodeStore,
            final String absPath,
            BiConsumer<DocumentNodeState, String> func
    ) {
        Accumulate(nodeStore, absPath, (DocumentNodeState nodeState, String path, Iterable<Void> res) -> {
            func.accept(nodeState, path);
            for (Void _ : res) {
            }
            return null;
        });
    }

    public static void PostOrder(
            final DocumentNodeStore nodeStore,
            final String absPath,
            BiConsumer<DocumentNodeState, String> func
    ) {
        Accumulate(nodeStore, absPath, (DocumentNodeState nodeState, String path, Iterable<Void> res) -> {
            for (Void _ : res) {
            }
            func.accept(nodeState, path);
            return null;
        });
    }

    private static <R> R Accumulate(
            final DocumentNodeState node,
            final String absPath,
            final TriFunction<DocumentNodeState, String, Iterable<R>, R> func
    ) {
        Iterable<String> childNodeNameIterable = node.getChildNodeNames();
        return func.apply(node, absPath, () -> {
            Iterator<String> childNodeNameIterator = childNodeNameIterable.iterator();
            return new Iterator<R>() {
                @Override
                public boolean hasNext() {
                    return childNodeNameIterator.hasNext();
                }

                @Override
                public R next() {
                    String childName = childNodeNameIterator.next();
                    return Accumulate(
                            (DocumentNodeState) node.getChildNode(childName),
                            PathUtils.concat(absPath, childName),
                            func
                    );
                }
            };
        });
    }

//    public static <R> R PostOrder(
//            final Tree node,
//            final BiFunction<Tree, Iterable<R>, R> func
//    ) {
//        Iterable<Tree> children = node.getChildren();
//        return func.apply(node, () -> {
//            Iterator<Tree> childrenIt = children.iterator();
//            return new Iterator<R>() {
//                @Override
//                public boolean hasNext() {
//                    return childrenIt.hasNext();
//                }
//
//                @Override
//                public R next() {
//                    return PostOrder(childrenIt.next(), func);
//                }
//            };
//        });
//    }
}
