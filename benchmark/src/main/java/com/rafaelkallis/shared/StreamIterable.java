package com.rafaelkallis.shared;

import java.util.Iterator;
import java.util.stream.Stream;

public class StreamIterable<T> implements Iterable<T> {

    final Stream<T> stream;

    public StreamIterable(Stream<T> stream) {
        this.stream = stream;
    }

    @Override
    public Iterator<T> iterator() {
        return stream.iterator();
    }
}
