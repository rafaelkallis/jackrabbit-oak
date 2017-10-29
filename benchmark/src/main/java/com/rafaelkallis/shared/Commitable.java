package com.rafaelkallis.shared;

import org.apache.jackrabbit.oak.api.CommitFailedException;

public interface Commitable<T> {
    T commit() throws CommitFailedException;
}
