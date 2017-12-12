package com.rafaelkallis.shared;

import static com.rafaelkallis.shared.Utils.childNodes;

import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class TraverseUtils {

    public static Iterable<Tree> LevelOrder(final Tree root) {
        return () -> {
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
        };
    }

    // public static Iterator<NodeState> LevelOrder(final NodeState root) {
    //     final MutableIterator<NodeState> nodes = new MutableIterator<>(root);
    //     return new Iterator<NodeState>() {
    //         @Override
    //         public boolean hasNext() {
    //             return nodes.hasNext();
    //         }

    //         @Override
    //         public NodeState next() {
    //             final NodeState next = nodes.next();
    //             final Iterator<String> childNodeNames = next.getChildNodeNames().iterator();
    //             nodes.append(new Iterator<NodeState>() {
    //                 @Override
    //                 public boolean hasNext() {
    //                     return childNodeNames.hasNext();
    //                 }

    //                 @Override
    //                 public NodeState next() {
    //                     return next.getChildNode(childNodeNames.next());
    //                 }
    //             });
    //             return next;
    //         }
    //     };
    // }

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

    // public static void PostOrder(
    //         final NodeStore nodeStore,
    //         final String absPath,
    //         BiConsumer<NodeState, String> func
    // ) {
    //     Accumulate(nodeStore, absPath, (NodeState nodeState, String path, Iterable<Void> res) -> {
    //             for (@SuppressWarnings("unused") Void ignore : res) {}
    //             func.accept(nodeState, path);
    //             return null;
    //         });
    // }

    public static <R> R Accumulate(
                                   Tree tree,
                                   BiFunction<Tree, Iterable<R>, R> func
                                   ) {
        Iterable<Tree> childIterable = tree.getChildren();
        return func.apply(tree, () -> {
                Iterator<Tree> childIterator = childIterable.iterator();
                return new Iterator<R>() {
                    @Override
                    public boolean hasNext() {
                        return childIterator.hasNext();
                    }
                    @Override
                    public R next() {
                        return Accumulate(childIterator.next(), func);
                    }
                };
            });
    }

    public static <R> R Accumulate(
            DocumentNodeState node,
            BiFunction<DocumentNodeState, Iterable<R>, R> func
    ) {
        Iterable<? extends ChildNodeEntry> childNodeIterable = node.getChildNodeEntries();
        return func.apply(node, () -> {
            Iterator<? extends ChildNodeEntry> childNodeIterator = childNodeIterable.iterator();
            return new Iterator<R>() {
                @Override
                public boolean hasNext() {
                    return childNodeIterator.hasNext();
                }

                @Override
                public R next() {
                    return Accumulate((DocumentNodeState) childNodeIterator.next().getNodeState(), func);
                }
            };
        });
    }

    public static Iterable<Tree> bottomUp(Tree root) {
        return () -> {
            Deque<Tree> s1 = new LinkedList<>();
            Deque<Tree> s2 = new LinkedList<>();
            s1.push(root);
            return new Iterator<Tree>() {
                @Override
                public boolean hasNext() {
                    return s1.size() > 0 || s2.size() > 0;
                }
                @Override
                public Tree next() {
                    while (s1.size() > 0 && (
                                             s2.size() == 0 ||
                                             PathUtils.isAncestor(s2.peek().getPath(),
                                                                  s1.peek().getPath()))) {
                        Tree n = s1.pop();
                        s2.push(n);
                        for (Tree child: n.getChildren()) {
                            s1.push(child);
                        }
                    }
                    return s2.pop();
                }
            };
        };
    }
}
