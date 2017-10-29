package com.rafaelkallis.shared;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

public class MutableIterator<T> implements Iterator<T> {

    private Iterator<T> valueIterator;
    private final Queue<Iterator<T>> fetchQueue;

    public MutableIterator() {
        this.valueIterator = Collections.emptyIterator();
        this.fetchQueue = new LinkedList<>();
    }

    public MutableIterator(Iterator<T> valIterator) {
        this();
        this.append(valIterator);
    }

    public MutableIterator(T val) {
        this();
        this.append(val);
    }

    public void append(T val) {
        this.fetchQueue.add(Collections.singleton(val).iterator());
    }

    public void append(Iterator<T> valIterator) {
        this.fetchQueue.add(valIterator);
    }

    @Override
    public boolean hasNext() {
        return valueIterator.hasNext() || !fetchQueue.isEmpty();
    }

    @Override
    public T next() {
        if (!valueIterator.hasNext()) {
            valueIterator = fetchQueue.poll();
        }
        return valueIterator.next();
    }
}
