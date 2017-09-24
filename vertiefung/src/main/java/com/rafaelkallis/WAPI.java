package com.rafaelkallis;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.ContentMirrorStoreStrategy;

import static org.apache.jackrabbit.oak.commons.PathUtils.getDepth;

public class WAPI {

    public static final int VOLATILITY_THRESHOLD = 5;
    public static final int SLIDING_WINDOW_LENGTH = 24 * 60 * 60 * 1000;

    private static DocumentNodeStore documentNodeStore = null;
    private static int volatilityThreshold;
    private static int slidingWindowLength;
    private static int clusterId;

    public static int getVolatilityThreshold() {
        if (!isWorkloadAware()) {
            throw new IllegalArgumentException();
        }
        return volatilityThreshold;
    }

    public static int getSlidingWindowLength() {
        if (!isWorkloadAware()) {
            throw new IllegalArgumentException();
        }
        return slidingWindowLength;
    }

    public static int getClusterId() {
        if (!isWorkloadAware()) {
            throw new IllegalArgumentException();
        }
        return clusterId;
    }

    public static void enableWorkloadAwareness(
            DocumentNodeStore documentNodeStore,
            int volatilityThreshold,
            int slidingWindowLength,
            int clusterId
    ) {
        WAPI.documentNodeStore = documentNodeStore;
        WAPI.volatilityThreshold = volatilityThreshold;
        WAPI.slidingWindowLength = slidingWindowLength;
        WAPI.clusterId = clusterId;
    }

    public static boolean isWorkloadAware() {
        return documentNodeStore != null;
    }

    public static NodeDocument getDocumentFromAbsPath(final String absPath){
        final String id = getDepth(absPath) + ":" + absPath;
        return documentNodeStore.getDocumentStore().find(Collection.NODES, id);
    }

    public static boolean isVolatile(final NodeDocument nodeDocument) {

        int count = 0;

        // assuming nodeDocument.getLocalDeleted().keySet() is sorted and is descending (recent -> old)
        for (Revision r : nodeDocument.getLocalDeleted().keySet()) {
            if (!isInSlidingWindow(r)){
                break;
            }
            if (!isVisible(r)){
                continue;
            }
            if (++count >= getVolatilityThreshold()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isVisible(Revision r) {
        return r.getClusterId() == getClusterId()
                || (r.compareRevisionTime(documentNodeStore
                .getHeadRevision()
                .getRevision(getClusterId())) < 0);
    }

    public static boolean isInSlidingWindow(Revision r){
        return System.currentTimeMillis() - getSlidingWindowLength() < r.getTimestamp();
    }
}
