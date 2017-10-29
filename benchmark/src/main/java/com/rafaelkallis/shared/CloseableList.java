package com.rafaelkallis.shared;

import java.util.ArrayList;

public class CloseableList<T extends AutoCloseable> extends ArrayList<T> implements AutoCloseable {

    public CloseableList() {
        super();
    }

    @Override
    public void close() throws Exception {
        for(AutoCloseable autoCloseable : this) {
            autoCloseable.close();
        }
    }
}
