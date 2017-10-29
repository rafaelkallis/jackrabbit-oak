package com.rafaelkallis.shared;

import java.util.function.Function;

public interface HigherOrderIterable<I, O> extends Function<Iterable<I>, Iterable<O>> {
}
