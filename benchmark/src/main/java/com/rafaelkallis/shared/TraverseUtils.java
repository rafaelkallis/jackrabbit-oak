package com.rafaelkallis.shared;

import static com.rafaelkallis.shared.Utils.childNodes;
import static com.rafaelkallis.shared.Utils.getNode;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

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
            // @Override
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
            final NodeStore nodeStore,
            final String absPath,
            final TriFunction<NodeState, String, Iterable<R>, R> func
    ) {
        return Accumulate(
                getNode(nodeStore, absPath),
                absPath,
                func
        );
    }

    public static void PreOrder(
            final NodeStore nodeStore,
            final String absPath,
            BiConsumer<NodeState, String> func
    ) {
        Accumulate(nodeStore, absPath, (NodeState nodeState, String path, Iterable<Void> res) -> {
                func.accept(nodeState, path);
                for (@SuppressWarnings("unused") Void ignore : res) {}
                return null;
            });
    }

    public static void PostOrder(
            final NodeStore nodeStore,
            final String absPath,
            BiConsumer<NodeState, String> func
    ) {
        Accumulate(nodeStore, absPath, (NodeState nodeState, String path, Iterable<Void> res) -> {
                for (@SuppressWarnings("unused") Void ignore : res) {}
                func.accept(nodeState, path);
                return null;
            });
    }

    private static <R> R Accumulate(
            final NodeState node,
            final String absPath,
            final TriFunction<NodeState, String, Iterable<R>, R> func
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
                            node.getChildNode(childName),
                            PathUtils.concat(absPath, childName),
                            func
                    );
                }
            };
        });
    }

    public static Iterable<Node> bottomUp(Node root) {
        return () -> {
            Deque<Node> s1 = new LinkedList<>();
            Deque<Node> s2 = new LinkedList<>();
            s1.push(root);
            return new Iterator<Node>() {

                @Override
                public boolean hasNext() {
                    return s1.size() > 0 || s2.size() > 0;
                }

                @Override
                public Node next() {
                    while (s1.size() > 0 &&
                           (s2.size() == 0 ||
                            PathUtils.isAncestor(s2.peek().path, s1.peek().path)
                            )) {
                        Node n = s1.pop();
                        s2.push(n);
                        for (ChildNodeEntry child: n.state.getChildNodeEntries()) {
                            s1.push(new Node(
                                             child.getNodeState(),
                                             PathUtils.concat(n.path, child.getName())
                                             ));
                        }
                    }
                    return s2.pop();
                }
            };
        };
    }

    public static Iterable<Node> bottomUp(NodeState nodeState, String path) {
        return bottomUp(new Node(nodeState, path));
    }
}
