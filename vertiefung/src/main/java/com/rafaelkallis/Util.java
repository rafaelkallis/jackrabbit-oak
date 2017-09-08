package com.rafaelkallis;

public class Util {
    public static void invariant(boolean predicate, String message) {
        if (!predicate) {
            System.err.println("\n=================================================");
            System.err.println(message);
            System.err.println("=================================================\n");
            throw new AssertionError(message);
        }
    }
}
