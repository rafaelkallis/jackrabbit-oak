package com.rafaelkallis.shared;

import com.mongodb.DB;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;
import java.net.UnknownHostException;
import java.util.function.Consumer;
import java.util.function.Function;

public class ClusterNode {

    private static final Logger LOG = LoggerFactory.getLogger(ClusterNode.class);

    private final ContentRepository contentRepository;
    private final int clusterId;

    private DocumentNodeStore nodeStore;

    public ClusterNode(
            final DocumentNodeStore nodeStore
    ) {
        this.clusterId = nodeStore.getClusterId();
        this.nodeStore = nodeStore;

        final Oak oak = new Oak(nodeStore)
                .with(new InitialContent())
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexEditorProvider());

        this.contentRepository = oak.createContentRepository();
    }

    public DocumentNodeStore getNodeStore() {
        return nodeStore;
    }

    public Commitable<Void> transaction(Consumer<Root> f) {
        return this.transaction((root -> {
            f.accept(root);
            return null;
        }));
    }

    public <T> Commitable<T> transaction(Function<Root, T> f) {
        final ContentSession contentSession = this.requestSession();
        final Root root = contentSession.getLatestRoot();
        final T result = f.apply(root);
        return () -> {
            root.commit();
            return result;
        };
    }

    public static Commitable<Void> singleTransaction(
            final DB db,
            final Consumer<Root> f
    ) throws UnknownHostException {
        return singleTransaction(
                db,
                (root) -> {
                    f.accept(root);
                    return null;
                }
        );
    }

    public static <T> Commitable<T> singleTransaction(
            final DB db,
            final Function<Root, T> f
    ) throws UnknownHostException {
        final DocumentNodeStore nodeStore = new DocumentMK.Builder()
                .setMongoDB(db)
                .getNodeStore();

        ClusterNode o = new ClusterNode(nodeStore);

        try {
            Commitable<T> commitable = o.transaction(f);
            return () -> {
                T result;
                try {
                    result = commitable.commit();
                } finally {
                    nodeStore.dispose();
                }
                return result;
            };
        } catch (Exception e) {
            nodeStore.dispose();
            throw e;
        }
    }

    private ContentSession requestSession() {
        try {
            return this.contentRepository.login(null, "default");
        } catch (LoginException | NoSuchWorkspaceException e) {
            LOG.error(e.getLocalizedMessage());
            throw new RuntimeException(String.format("ClusterNode %d failed to start session", this.clusterId));
        }
    }
}
