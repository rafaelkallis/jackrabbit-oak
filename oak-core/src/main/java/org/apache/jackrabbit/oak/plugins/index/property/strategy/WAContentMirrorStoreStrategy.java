package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

import static com.google.common.collect.Iterables.*;
import static org.apache.jackrabbit.oak.plugins.document.util.Utils.abortingIterable;

/**
 * Workload Aware, Mirror, Store Strategy
 */
public final class WAContentMirrorStoreStrategy extends ContentMirrorStoreStrategy implements IndexStoreStrategy {

    public static final int DEFAULT_VOLATILITY_THRESHOLD = 5;
    public static final int DEFAULT_SLIDING_WINDOW_LENGTH = 24 * 60 * 60 * 1000;

    private final DocumentNodeStore documentNodeStore;

    private final int volatilityThreshold;
    private final int slidingWindowLength;

    public WAContentMirrorStoreStrategy(String indexName,
                                        @Nonnull DocumentNodeStore documentNodeStore,
                                        @Nonnegative int volatilityThreshold,
                                        @Nonnegative int slidingWindowLength
    ) {
        super(indexName);
        this.documentNodeStore = documentNodeStore;
        this.volatilityThreshold = volatilityThreshold;
        this.slidingWindowLength = slidingWindowLength;
    }

    @Override
    public void update(Supplier<NodeBuilder> index,
                       String propertyIndexPath,
                       @Nullable String indexName,
                       @Nullable NodeBuilder indexMeta,
                       Set<String> beforeKeys,
                       Set<String> afterKeys) {

        for (String key : beforeKeys) {
            if (isVolatile(truncate(propertyIndexPath))) {
                // remove match property
                NodeBuilder builder = index.get().getChildNode(key);
                for (String label : PathUtils.elements(propertyIndexPath)) {
                    builder = builder.getChildNode(label);
                }
                if (builder.exists()) {
                    builder.removeProperty("match");
                }
            } else {
                super.remove(index.get(), key, propertyIndexPath);
            }
        }
        for (String key : afterKeys) {
            super.insert(index.get(), key, propertyIndexPath);
        }
    }

    private boolean isVolatile(@Nonnull String path) {
        Predicate<Revision> isInSlidingWindow = new Predicate<Revision>() {
            @Override
            public boolean apply(@Nullable Revision revision) {
                return revision.getTimestamp() > System.currentTimeMillis() - WAContentMirrorStoreStrategy.this.slidingWindowLength;
            }
        };
        Predicate<Revision> isFromThisClusterNode = new Predicate<Revision>() {
            @Override
            public boolean apply(@Nullable Revision revision) {
                return revision.getClusterId() == WAContentMirrorStoreStrategy.this.documentNodeStore.getClusterId();
            }
        };

        NodeDocument documentNode = this.documentNodeStore.getDocumentStore().find(Collection.NODES, path);
        if (documentNode == null) {
            return false;
        }

        Iterable<Revision> revisions = documentNode.getLocalDeleted().keySet();
        revisions = abortingIterable(revisions, isInSlidingWindow);
        revisions = filter(revisions, isFromThisClusterNode);
        revisions = skip(revisions, this.volatilityThreshold);

        return !isEmpty(revisions);
    }


    /**
     * Truncates the value from the path.
     * "/now/a/b" -> "/a/b"
     *
     * @param propertyIndexPath the path in the property index
     * @return the truncated path
     */
    private static @Nonnull
    String truncate(String propertyIndexPath) {
        return propertyIndexPath.substring(propertyIndexPath.indexOf("/", 1));
    }
}
