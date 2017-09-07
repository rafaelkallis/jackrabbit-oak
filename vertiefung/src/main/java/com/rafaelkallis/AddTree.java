package com.rafaelkallis;

import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;

import java.util.function.Function;

public class AddTree implements Function<Root, Root> {

    @Override
    public Root apply(Root root) {

        Tree home = root.getTree("/").addChild("home");
        Tree news = home.addChild("news");
        Tree loans = home.addChild("loans");
        Tree breaking = news.addChild("breaking");
        Tree rates = loans.addChild("rates");

        return root;
    }
}
