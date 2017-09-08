package com.rafaelkallis;

import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Result;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.query.xpath.XPathToSQL2Converter;

import javax.jcr.query.Query;
import java.text.ParseException;
import java.util.Collections;

import static com.rafaelkallis.Util.invariant;

public class EntryPoint {

    public static void main(String[] args) {
        performAvoidableConflict();
        performQuery();
    }

    private static void performAvoidableConflict() {
        // assume
        invariant(ClusterNode
                .simpleWrite(root -> {
                    root.getTree("/").addChild("home");
                    root.getTree("/home").addChild("news");
                    root.getTree("/home").addChild("loans");
                    root.getTree("/home/news").addChild("breaking");
                    root.getTree("/home/loans").addChild("rates");
                    root.getTree("/home/news/breaking").setProperty("pub", "now");
                })
                .commit(), "t3 failed");


        // act
        Commitable t4 = ClusterNode.simpleWrite(root -> root.getTree("/home/news/breaking").removeProperty("pub"));
        Commitable t5 = ClusterNode.simpleWrite(root -> root.getTree("/home/loans/rates").setProperty("pub", "now"));
        invariant(t4.commit() && t5.commit(), "t4 or t5 failed");

        // assert
        invariant(ClusterNode.simpleWrite(root -> {
            invariant(root.getTree("/home/news/breaking").getProperty("pub") == null, "/home/news/breaking still has property \"pub: now\"");
            invariant(root.getTree("/home/loans/rates").getProperty("pub") != null, "/home/loans/rates doesn't have property \"pub: now\"");
        }).commit(), "check failed");

        // cleanup
        invariant(ClusterNode.simpleWrite(root -> {
            root.getTree("/home/news/breaking").remove();
            root.getTree("/home/news").remove();
            root.getTree("/home/loans/rates").remove();
            root.getTree("/home/loans").remove();
            root.getTree("/home").remove();
        }).commit(), "cleanup failed");
    }

    private static void performQuery() {
        // assume
        invariant(ClusterNode
                .simpleWrite(root -> {
                    root.getTree("/").addChild("home");
                    root.getTree("/home").addChild("news");
                    root.getTree("/home/news").addChild("breaking");
                    root.getTree("/home/news/breaking").setProperty("pub", "now");
                })
                .commit(), "assume failed");

        // act
        invariant(ClusterNode.simpleWrite(root -> {
            try {
                final Result result = root.getQueryEngine()
                        .executeQuery(
                                new XPathToSQL2Converter().convert("//*[@pub='now']"),
                                Query.JCR_SQL2,
                                Collections.emptyMap(),
                                Collections.emptyMap()
                        );

                ResultRow[] resultRows = Iterables.toArray(result.getRows(), ResultRow.class);

                // assert
                invariant(resultRows.length == 1, String.format("Invalid number of result rows. Expected 1, got %d", resultRows.length));
                invariant("/home/news/breaking".equals(resultRows[0].getPath()), String.format("Invalid path returned. Expected /home/news/breaking, got %s", resultRows[0].getPath()));
            } catch (ParseException e) {
                invariant(false, e.getMessage());
            }

        }).commit(), "act failed");

        invariant(ClusterNode.simpleWrite(root -> {
            root.getTree("/home/news/breaking").remove();
            root.getTree("/home/news").remove();
            ;
            root.getTree("/home").remove();
        }).commit(), "cleanup failed");
    }
}
