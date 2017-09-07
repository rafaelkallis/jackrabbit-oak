package com.rafaelkallis;

import org.apache.jackrabbit.oak.api.Root;

import java.util.function.Function;

public class RemoveTree implements Function<Root, Root> {

    @Override
    public Root apply(Root root) {

        root.getTree("/home/news/breaking").remove();
        root.getTree("/home/news").remove();
        root.getTree("/home/loans/rates").remove();
        root.getTree("/home/loans").remove();
        root.getTree("/home").remove();
        
        return root;
    }
}
