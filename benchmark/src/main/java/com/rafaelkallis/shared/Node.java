package com.rafaelkallis.shared;

import org.apache.jackrabbit.oak.spi.state.NodeState;

public class Node {
    public final NodeState state;
    public final String path;

    public Node(NodeState state, String path) {
        this.state = state;
        this.path = path;
    }
}
