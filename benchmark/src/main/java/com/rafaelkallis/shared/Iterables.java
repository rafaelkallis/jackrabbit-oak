package com.rafaelkallis.shared;

import com.google.common.collect.Iterators;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.UniformIntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.Iterator;

import static com.google.common.collect.Iterators.transform;

public class Iterables {

    public static Iterable<Integer> zipf(double skew) {
        IntegerDistribution zipf = new ZipfDistribution(Integer.MAX_VALUE, skew);
        return () -> new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                return zipf.sample();
            }
        };
    }


    public static Iterable<Integer> uniform() {
        IntegerDistribution unif = new UniformIntegerDistribution(1, Integer.MAX_VALUE);
        return () -> new Iterator<Integer>() {
            @Override
            public boolean hasNext() {
                return true;
            }

            @Override
            public Integer next() {
                return unif.sample();
            }
        };
    }

    public static <T> HigherOrderIterable<T, T> limit(int amount) {
        return (source) -> () -> Iterators.limit(source.iterator(), amount);
    }

    public static HigherOrderIterable<Integer, String> toStr() {
        return (source) -> () -> transform(
                source.iterator(),
                Object::toString
        );
    }

    public static HigherOrderIterable<String, HashCode> hash() {
        return (source) -> () -> transform(
                source.iterator(),
                x -> Hashing.murmur3_32().hashBytes(x.getBytes())
        );
    }

    public static HigherOrderIterable<String, String> concatTimeBucket(long startingTime, long interval) {
        return (source) -> () -> transform(
                source.iterator(),
                x -> x.concat(Long.toString((System.currentTimeMillis() - startingTime) / interval))
        );
    }

    public static HigherOrderIterable<HashCode, Integer> consistentHash(int buckets) {
        return (source) -> () -> transform(
                source.iterator(),
                x -> Hashing.consistentHash(x, buckets)
        );
    }

    public static <T> HigherOrderIterable<Integer, T> pick(T[] elements) {
        return (source) -> () -> transform(
                source.iterator(),
                x -> elements[x]
        );
    }
}
