package com.rafaelkallis.shared;

public class ConcurrentValue<T> {
    private T value;

    public ConcurrentValue(T value) {
        this.value = value;
    }

    public synchronized T getValue() {
        return value;
    }

    public synchronized void setValue(T value) {
        this.value = value;
    }
}
