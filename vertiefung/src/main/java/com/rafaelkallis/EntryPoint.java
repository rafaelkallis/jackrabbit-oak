package com.rafaelkallis;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.*;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.bson.Document;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

public class EntryPoint {

    static MongoClient mongoClient;
    static DocumentNodeStore nodeStore;
    static Oak oak;
    static ContentRepository contentRepository;

    static Runnable[] tasks = {
            /**
             * Insert document in mongo
            */
            new Runnable() {
                public void run() {
                    MongoDatabase mongoDatabase = mongoClient.getDatabase("temp");
                    try {
                        MongoCollection<Document> collection = mongoDatabase.getCollection("temp");
                        try {
                            collection.insertOne(new Document("key", "val"));
                            invariant(collection.count() == 1, "document was not inserted");
                            for (Document document : collection.find()) {
                                invariant(document.get("key").toString().equals("val"), "property key did not have value \"val\"");
                            }
                        } finally {
                            collection.drop();
                        }
                    } finally {
                        mongoDatabase.drop();
                    }
                }
            },
            /**
             * Initialize OAK
            */
            new Runnable() {
                public void run() {
                    invariant(makeSession().getLatestRoot().getTree("/").exists(), "root doesn't exist");
                }
            },
            /**
             * create & remove tree from figure 3
            */
            new Runnable() {
                @Override
                public void run() {
                    try {
                        ContentSession contentSession = makeSession();
                        Root root;

                        root = new AddTree().apply(contentSession.getLatestRoot());
                        root.commit();

                        root = contentSession.getLatestRoot();
                        invariant(root.getTree("/home").exists(), "/home does not exist");
                        invariant(root.getTree("/home/news").exists(), "/home/news does not exist");
                        invariant(root.getTree("/home/news/breaking").exists(), "/home/news/breaking does not exist");
                        invariant(root.getTree("/home/loans").exists(), "/home/loans does not exist");
                        invariant(root.getTree("/home/loans/rates").exists(), "/home/loans/rates does not exist");

                        root = new RemoveTree().apply(contentSession.getLatestRoot());
                        root.commit();

                        root = contentSession.getLatestRoot();
                        invariant(!root.getTree("/home").exists(), "/home does not exist");
                        invariant(!root.getTree("/home/news").exists(), "/home/news does not exist");
                        invariant(!root.getTree("/home/news/breaking").exists(), "/home/news/breaking does not exist");
                        invariant(!root.getTree("/home/loans").exists(), "/home/loans does not exist");
                        invariant(!root.getTree("/home/loans/rates").exists(), "/home/loans/rates does not exist");
                    } catch (CommitFailedException e) {
                        invariant(false, "create & remove tree: commit failed");
                    }
                }
            },
            /**
             * avoidable conflict
            */
            new Runnable() {
                @Override
                public void run() {

                    // G^3
                    Thread t3 = new Thread(() -> {
                        Root root = makeSession().getLatestRoot();
                        new AddTree().apply(root);
                        root.getTree("/home/news/breaking").setProperty("pub", "now");
                        try {
                            root.commit();
                        } catch (CommitFailedException e) {
                            invariant(false, "create G^3: commit failed");
                        }
                    });

                    // G^4
                    Thread t4 = new Thread(() -> {
                        Root root = makeSession().getLatestRoot();
                        root.getTree("/home/news/breaking").removeProperty("pub");
                        try {
                            root.commit();
                        } catch (CommitFailedException e) {
                            invariant(false, "create G^4: commit failed");
                        }
                    });

                    // G^5
                    Thread t5 = new Thread(() -> {
                        Root root = makeSession().getLatestRoot();
                        root.getTree("/home/loans/rates").setProperty("pub", "now");
                        try {
                            root.commit();
                        } catch (CommitFailedException e) {
                            invariant(false, "create G^5: commit failed");
                        }
                    });

                    t3.start();

                    try {
                        t3.join();
                    } catch (InterruptedException e) {
                        invariant(false, "create G^3: interrupted");
                    }

                    t4.start();
                    t5.start();

                    try {
                        t4.join();
                    } catch (InterruptedException e) {
                        invariant(false, "create G^4: interrupted");
                    }
                    try {
                        t5.join();
                    } catch (InterruptedException e) {
                        invariant(false, "create G^5: interrupted");
                    }

                    Root root = makeSession().getLatestRoot();

                    invariant(root.getTree("/home/news/breaking").getProperty("pub") == null, "/home/news/breaking still has property \"pub: now\"");
                    invariant(root.getTree("/home/loans/rates").getProperty("pub") != null, "/home/loans/rates doesn't have property \"pub: now\"");
                }
            }

    };

    static ContentSession makeSession() {
        ContentSession contentSession = null;
        try {
            contentSession = contentRepository.login(null, "default");
        } catch (LoginException | NoSuchWorkspaceException e) {
            invariant(false, e.toString());
        }
        return contentSession;
    }

    static void invariant(boolean predicate, String message) {
        if (!predicate) {
            System.err.println("\n=================================================");
            System.err.println(message);
            System.err.println("=================================================\n");
            throw new RuntimeException(message);
        }
    }

    static void beforeAll() {
        mongoClient = new MongoClient();
        nodeStore = new DocumentMK.Builder()
                .setMongoDB(mongoClient.getDB("oak"))
                .getNodeStore();
        oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider())
                .with(new PropertyIndexProvider());
        contentRepository = oak.createContentRepository();
    }

    static void afterAll() {
        nodeStore.dispose();
        mongoClient.close();
    }

    public static void main(String[] args) {
        beforeAll();
        try {
            for (Runnable task : tasks) {
                task.run();
            }
        } finally {
            afterAll();
        }
    }
}
