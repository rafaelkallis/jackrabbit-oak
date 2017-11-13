package com.rafaelkallis;

import static com.rafaelkallis.shared.TraverseUtils.Accumulate;
import static com.rafaelkallis.shared.TraverseUtils.LevelOrder;
import static com.rafaelkallis.shared.Utils.firstNode;
import static com.rafaelkallis.shared.Utils.initializePropertyIndex;
import static com.rafaelkallis.shared.Utils.lastNode;
import static com.rafaelkallis.shared.Utils.mapToPath;
import static com.rafaelkallis.shared.Utils.memoize;
import static com.rafaelkallis.shared.Utils.millisSinceNow;
import static com.rafaelkallis.shared.Utils.setUpCompleteTree;
import static com.rafaelkallis.shared.Utils.throwException;
import static com.rafaelkallis.shared.Utils.totalNodes;
import static com.rafaelkallis.shared.Utils.toggleTwice;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.rafaelkallis.shared.ClusterNode;
import com.rafaelkallis.shared.Consumer;
import com.rafaelkallis.shared.Utils;

public class App {

    static final Logger LOG = LoggerFactory.getLogger(App.class);

    static final int templateFanout = Integer.getInteger("templateFanout", 2);
    static final int templateDepth = Integer.getInteger("templateDepth", 20);
    static final String templateName = System.getProperty("templateName", "oak-template");

    static final int fanout = Integer.getInteger("fanout", 2);
    static final int depth = Integer.getInteger("depth", 20);
    static final int skew = Integer.getInteger("skew", 1);
    static final int topLevels = Integer.getInteger("topLevels", 1);
    static final int bottomLevels = Integer.getInteger("bottomLevels", 1);
    static final int clusterId = Integer.getInteger("clusterId", 1);
    static final int volatilityThreshold = Integer.getInteger("volatilityThreshold", 5);
    static final int slidingWindowLength = Integer.getInteger("slidingWindowLength", 30 * 1000);

    static final int writesPerTick = Integer.getInteger("writesPerTick", 10);
    static final int queriesPerTick = Integer.getInteger("queriesPerTick", 1);

    static final long experimentMillis = Long.getLong("experimentMillis", 5 * 60 * 1000);
    static final long workloadMillis = Long.getLong("workloadMillis", 30 * 1000);
    static final long experimentTicks = Long.getLong("experimentTicks", 2000);

    static final String mongoUri = System.getProperty("mongoUri", "mongodb://localhost");
    static final String mongoName = System.getProperty("mongoName", "oak");
    static final String contentRootPath = System.getProperty("contentRootPath", "/content");

    static final Boolean skipInitialize = Boolean.getBoolean("skipInitialize");
    static final boolean generateDataset = Boolean.getBoolean("generateDataset");

    static final String dataset = System.getProperty("dataset", "synthetic");
    static final boolean syntheticDataset = dataset.equals("synthetic");
    static final boolean aemDataset = dataset.equals("aem");
    static final String outFileName = System.getProperty("outFileName", String.format("output_%d_%s_tau%d_L%d", System.currentTimeMillis() / 1000L, dataset, volatilityThreshold, slidingWindowLength));
    static final boolean QTP = Boolean.getBoolean("QTP");
    static final boolean GC = Boolean.getBoolean("GC");
    static final long GCPeriodicity = Long.getLong("GCPeriodicity", 30 * 1000);

    static final String loopMode = System.getProperty("loopMode", "millis");
    static final boolean millisLoopMode = loopMode.equals("millis");
    static final boolean tickLoopMode = loopMode.equals("tick");

    static final boolean skipExperiment = Boolean.getBoolean("skipExperiment");
    static final int templateClusterId = Integer.getInteger("templateClusterId", 100);

    public static void main(String[] args) throws CommitFailedException, IOException, ParseException {

        LOG.debug("loopMode: {}", loopMode);
        LOG.debug("fanout: {}", fanout);
        LOG.debug("depth: {}", depth);
        LOG.debug("templateFanout: {}", templateFanout);
        LOG.debug("templateDepth: {}", templateDepth);
        LOG.debug("templateName: {}", templateName);
        LOG.debug("topLevels: {}", topLevels);
        LOG.debug("bottomLevels: {}", bottomLevels);
        LOG.debug("clusterId: {}", clusterId);
        LOG.debug("volatilityThreshold: {}", volatilityThreshold);
        LOG.debug("slidingWindowLength: {}", slidingWindowLength);
        LOG.debug("skew: {}", skew);
        LOG.debug("writesPerTick: {}", writesPerTick);
        LOG.debug("queriesPerTick: {}", queriesPerTick);
        LOG.debug("experimentMillis: {}", experimentMillis);
        LOG.debug("workloadMillis: {}", workloadMillis);
        LOG.debug("mongoUri: {}", mongoUri);
        LOG.debug("mongoName: {}", mongoName);
        LOG.debug("contentRootPath: {}", contentRootPath);
        LOG.debug("outFileName: {}", outFileName);
        LOG.debug("skipInitialize: {}", skipInitialize);
        LOG.debug("dataset: {}", dataset);
        LOG.debug("GC: {}", GC);
        LOG.debug("GCPeriodicity: {}", GCPeriodicity);
        LOG.debug("QTP: {}", QTP);

        try (final MongoClient mongoClient = new MongoClient()) {
            final DB db = mongoClient.getDB(mongoName);

            if (generateDataset) {
                LOG.debug("generating synthetic dataset");
                try (final DocumentNodeStore nodeStore = new DocumentMK.Builder().setMongoDB(mongoUri, templateName, 16)
                     .setClusterId(templateClusterId).setVolatilityThreshold(volatilityThreshold)
                     .setSlidingWindowLength(slidingWindowLength).getNodeStore()) {
                    ClusterNode clusterNode = new ClusterNode(nodeStore);
                    clusterNode.transaction(root -> {
                            LOG.debug("initializing property index");
                            initializePropertyIndex(root, "pub");
                            LOG.debug("creating synthetic tree");
                            Tree r = root.getTree("/");
                            for (String label : PathUtils.elements(contentRootPath)) {
                                r = r.addChild(label);
                            }
                            setUpCompleteTree(r, templateFanout, templateDepth);
                        }).commit();
                }
            }

            if (!skipInitialize) {
                LOG.debug("dropping {}", mongoName);
                mongoClient.dropDatabase(mongoName);

                if (fanout == templateFanout && depth == templateDepth) {
                    LOG.debug("cloning {} to {}", templateName, mongoName);
                    final Bson command = BsonDocument.parse(
                                                            String.format("{ copydb: 1, fromdb: \"%s\", todb: \"%s\"}", templateName, mongoName));
                    mongoClient.getDatabase("admin").runCommand(command);
                    LOG.debug("cloning finished");
                } else {
                    LOG.debug("creating new database");
                    ClusterNode.singleTransaction(db, root -> {
                            LOG.debug("initializing property index");
                            initializePropertyIndex(root, "pub");
                            LOG.debug("creating synthetic tree");
                            Tree tree = root.getTree("/");
                            for (String child : PathUtils.elements(contentRootPath)) {
                                tree = tree.addChild(child);
                            }
                            setUpCompleteTree(tree, fanout, depth);
                            LOG.debug("setUp finished");
                        }).commit();
                    LOG.debug("setUp committed");
                }
            }

            if (skipExperiment) {
                return;
            }

            // final String[] topNodes = ClusterNode.singleTransaction(
            // db,
            // root -> {
            // return slice(root.getTree(contentRootPath), 0, topLevels);
            // }
            // ).commit();
            //
            // final String[] bottomNodes = ClusterNode.singleTransaction(
            // db,
            // root -> {
            // return slice(root.getTree(contentRootPath), depth - bottomLevels, depth);
            // }
            // ).commit();
            final DB templateDB = mongoClient.getDB(templateName);
            try (final DocumentNodeStore nodeStore = new DocumentMK.Builder()
                 .setMongoDB(mongoUri, mongoName, 16)
                 .setClusterId(clusterId)
                 .setVolatilityThreshold(volatilityThreshold)
                 .setSlidingWindowLength(slidingWindowLength)
                 .memoryCacheSize(1 * 1024 * 1024 * 1024)
                 // .setPropertyIndexCleanUpProps("pub")
                 .getNodeStore();
                 final CSVPrinter tickData = new CSVPrinter(new FileWriter(outFileName), CSVFormat.DEFAULT)) {
                tickData.printRecord("tick", "timestamp", "query_runtime", "trav_index_nodes", "trav_vol_nodes", "trav_unprod_nodes");

                final ClusterNode clusterNode = new ClusterNode(nodeStore);

                final Timer gcTimer = new Timer();

                final Supplier<String[]> aemNodesSupplier = aemNodes(clusterNode);

                final AtomicLong experimentTickCounter = new AtomicLong();

                final Supplier<Long> workloadSupplier = millisAwareWorkload();

                final Supplier<String> writeNodePaths = syntheticDataset
                    ? syntheticTreeWriteNodePaths(workloadSupplier)
                    : aemDataset
                    ? aemWriteNodePaths(aemNodesSupplier.get(), workloadSupplier)
                    : throwException("no write node path supplier");

                final Supplier<String> queryNodePaths = syntheticDataset
                    ? syntheticTreeQueryNodePaths(workloadSupplier)
                    : aemDataset
                    ? aemQueryNodePaths(aemNodesSupplier.get(), workloadSupplier)
                    : throwException("no query node path supplier");

                // final Supplier<Long> millisSinceWarmUpStart = millisSinceNow();

                // if (GC) {
                // 	gcTimer.schedule(gcTimerTask(nodeStore, "/oak:index/pub/:index/pub", Consumer.Two.noOp()), 0,
                // 			GCPeriodicity);
                // }

                // LOG.debug("warm-up starting");
                // if (millisMode) {
                // 	LOG.debug("warm-up finishing at {}", Instant.now().plusMillis(warmUpMillis));
                // }

                // final Consumer.One<Long> warmUpTick = warmUpTickFactory(clusterNode, nodeStore, writeNodePaths,
                // 		queryNodePaths);

                // final Supplier<Boolean> warmUpLoopCondition = millisMode
                // 		? () -> millisSinceWarmUpStart.get() < warmUpMillis
                // 		: tickMode ? () -> warmUpTickCounter.get() < warmUpTicks
                // 				: throwException("no warmup loop condition provider");

                // while (warmUpLoopCondition.get()) {
                // 	long currentTick = warmUpTickCounter.getAndIncrement();
                // 	warmUpTick.accept(currentTick);
                // 	if (tickMode && currentTick % 50 == 0) {
                // 		LOG.debug("{}/{} ticks complete", currentTick, warmUpTicks);
                // 	}
                // }

                // LOG.debug("warm-up finished");

                final Supplier<Long> millisSinceExperimentStart = millisSinceNow();

                LOG.debug("experiment starting");
                if (millisLoopMode) {
                    LOG.debug("experiment finishing at {}", Instant.now().plusMillis(experimentMillis));
                }

                // final Consumer.One<String> dataLogger = dataLoggerFactory(out, millisMode
                // ? millisSinceExperimentStart
                // : tickMode
                // ? experimentTickCounter::get
                // : throwException("no time supplier")
                // );

                if (GC) {
                    // gcTimer.cancel();
                    gcTimer.schedule(
                                     gcTimerTask(nodeStore, "/oak:index/pub/:index/pub", (Long runtime, Long nCleanedNodes) -> {
                                             // if (benchmarkGCMillis) {
                                             // dataLogger.accept(delta.toString());
                                             // }
                                             // if (benchmarkGCCleanedNodes) {
                                             // dataLogger.accept(nCleanedNodes.toString());
                                             // }
                                         }), 0, GCPeriodicity);
                }

                final Consumer.One<Long> experimentTick = experimentTickFactory(
                                                                                clusterNode,
                                                                                nodeStore,
                                                                                writeNodePaths,
                                                                                queryNodePaths,
                                                                                millisSinceExperimentStart::get,
                                                                                (tick,
                                                                                 timestamp,
                                                                                 queryRuntime,
                                                                                 travIndexNodex,
                                                                                 travVolatileNodes,
                                                                                 travUnproductiveNodes) -> {
                                                                                    try {
                                                                                        tickData.printRecord(
                                                                                                             tick,
                                                                                                             timestamp,
                                                                                                             queryRuntime,
                                                                                                             travIndexNodex,
                                                                                                             travVolatileNodes,
                                                                                                             travUnproductiveNodes);
                                                                                    } catch (IOException e) {
                                                                                        LOG.error("io exception", e);
                                                                                    }
                                                                                });

                final Supplier<Boolean> experimentLoopCondition = millisLoopMode
                    ? () -> millisSinceExperimentStart.get() < experimentMillis
                    : tickLoopMode
                    ? () -> experimentTickCounter.get() < experimentTicks
                    : throwException("no experiment loop condition supplier");

                while (experimentLoopCondition.get()) {
                    final long currentTick = experimentTickCounter.getAndIncrement();
                    experimentTick.accept(currentTick);
                    if (tickLoopMode && currentTick % 50 == 0) {
                        LOG.debug("{}/{} ticks complete", currentTick, experimentTicks);
                    }
                }

                LOG.debug("experiment finished");
                gcTimer.cancel();
            }
        }
    }

    // public static Consumer.One<Long> warmUpTickFactory(final ClusterNode clusterNode, final DocumentNodeStore nodeStore,
    // 		final Supplier<String> writeNodePaths, final Supplier<String> queryNodePaths) {

    // 	final Consumer.One<String> writeOp = toggleProperty(clusterNode);
    // 	final Function<String, Set<String>> queryOp = externalQuery(nodeStore, Consumer.Four.noOp());
    // 	return tickFactory(writeNodePaths, writeOp, writesPerTick, queryNodePaths, queryOp, queriesPerTick,
    // 			Consumer.One.noOp());
    // }

    public static Consumer.One<Long> experimentTickFactory(
                                                           final ClusterNode clusterNode,
                                                           final DocumentNodeStore nodeStore,
                                                           final Supplier<String> writeNodePaths,
                                                           final Supplier<String> queryNodePaths,
                                                           final Supplier<Long> experimentMillisSupplier,
                                                           final Consumer.Six<Long, Long, Double, Double, Double, Double> hook
                                                           ) {
        final AtomicReference<Mean> meanQueryRuntime = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedIndexNodes = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedVolatileIndexNodes = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedUnproductiveIndexNodes = new AtomicReference<>(new Mean());

        final Consumer.One<String> writeOp = toggleTwice(clusterNode);

        final Function<String, Set<String>> queryOp = externalQuery(nodeStore,
                                                                    (queryRuntime, traversedIndexNodes, traversedVolatileNodes, traversedUnproductiveNodes) -> {
                                                                        meanQueryRuntime.get().increment(queryRuntime);
                                                                        meanTraversedIndexNodes.get().increment(traversedIndexNodes);
                                                                        meanTraversedVolatileIndexNodes.get().increment(traversedVolatileNodes);
                                                                        meanTraversedUnproductiveIndexNodes.get().increment(traversedUnproductiveNodes);
                                                                    });

        return tickFactory(writeNodePaths, writeOp, queryNodePaths, queryOp, (tick) -> {
                hook.accept(
                            tick,
                            experimentMillisSupplier.get(),
                            meanQueryRuntime.get().getResult(),
                            meanTraversedIndexNodes.get().getResult(),
                            meanTraversedVolatileIndexNodes.get().getResult(),
                            meanTraversedUnproductiveIndexNodes.get().getResult()
                            );
            });
    }

    public static Consumer.One<Long> tickFactory(Supplier<String> writeNodePaths,
                                                 Consumer.One<String> writeOp,
                                                 Supplier<String> queryNodePaths,
                                                 Function<String,
                                                 Set<String>> queryOp,
                                                 Consumer.One<Long> hook) {
        return (tick) -> {

            // update
            for (int i = 0; i < writesPerTick; i++) {
                writeOp.accept(writeNodePaths.get());
            }

            // query
            for (int i = 0; i < queriesPerTick; i++) {
                queryOp.apply(queryNodePaths.get());
            }

            hook.accept(tick);
        };
    }

    /**
     * Runs Query in external mode.
     *
     * @param documentNodeStore
     * @param hook:
     *            A function which accepts 1) the query execution time as 1st
     *            parameter and 2) the number of unproductive nodes encountered
     *            during execution as 2nd parameter
     * @return A function which accepts a path and returns the set of content node
     *         paths which 1) are descendants of the provided path and 2) have
     *         property "pub" set to "now"
     */
    public static Function<String, Set<String>> externalQuery(final DocumentNodeStore documentNodeStore,
                                                              final Consumer.Four<Long, Long, Long, Long> hook) {
        final String parentPath = "/oak:index/pub/:index/now";
        return (String contentPath) -> {
            final Set<String> resultSet = new HashSet<>();
            final Supplier<Long> runtime = millisSinceNow();
            final AtomicLong traversedIndexNodes = new AtomicLong();
            final AtomicLong traversedVolatileNodes = new AtomicLong();
            final AtomicLong traversedUnproductiveNodes = new AtomicLong();
            Accumulate(documentNodeStore, PathUtils.concat(parentPath, PathUtils.relativize("/", contentPath)),
                       (NodeState node, String path, Iterable<Boolean> accumulator) -> {
                           final boolean isMatching = node.getBoolean("match");
                           final boolean isVolatile = node instanceof DocumentNodeState
                               && ((DocumentNodeState) node).isVolatile();
                           if (isMatching) {
                               resultSet.add(PathUtils.relativize(parentPath, path));
                           }
                           boolean isUnproductive = !isMatching && !isVolatile;
                           for (boolean isChildUnproductive : accumulator) {
                               isUnproductive &= isChildUnproductive;
                           }

                           if (isUnproductive && QTP) {
                               node.builder().remove();
                           }

                           traversedIndexNodes.getAndIncrement();
                           if (isVolatile) {
                               traversedVolatileNodes.getAndIncrement();
                           } else if (isUnproductive) {
                               traversedUnproductiveNodes.getAndIncrement();
                           }

                           return isUnproductive;
                       });
            hook.accept(runtime.get(), traversedIndexNodes.get(), traversedVolatileNodes.get(),
                        traversedUnproductiveNodes.get());
            return resultSet;
        };
    }

    /**
     * Cleans all unproductive nodes under the given path
     *
     * @param nodeStore
     * @param absPath
     * @param hook:A
     *            function which accepts 1) the cleanup execution time as 1st
     *            parameter and 2) the number of nodes cleaned during execution as
     *            2nd parameter
     * @return
     */
    public static TimerTask gcTimerTask(DocumentNodeStore nodeStore, String absPath, Consumer.Two<Long, Long> hook) {
        return new TimerTask() {
            @Override
            public void run() {
                final Supplier<Long> delta = millisSinceNow();
                final AtomicLong nCleanedNodes = new AtomicLong();
                Accumulate(nodeStore, absPath, (NodeState node, String path, Iterable<Boolean> accumulator) -> {
                        boolean hasChild = false;
                        boolean isMatching = node.getBoolean("match");
                        boolean isVolatile = node instanceof DocumentNodeState && ((DocumentNodeState) node).isVolatile();
                        for (boolean isChildRemoved : accumulator) {
                            hasChild |= !isChildRemoved;
                        }
                        if (!hasChild && !isMatching && !isVolatile) {
                            nCleanedNodes.getAndIncrement();
                            node.builder().remove();
                            return true;
                        }
                        return false;
                    });
                hook.accept(delta.get(), nCleanedNodes.get());
            }
        };
    }

    public static Supplier<Long> millisAwareWorkload() {
        final Supplier<Long> delta = millisSinceNow();
        return () -> delta.get() / workloadMillis;
    }

    public static Supplier<Long> noWorkload() {
        return () -> 0L;
    }

    public static Supplier<String[]> aemNodes(final ClusterNode o) {
        return memoize(() -> {
                final LinkedList<String> list = new LinkedList<>();
                try {
                    o.transaction(root -> {
                            LevelOrder(root.getTree(contentRootPath), node -> list.add(node.getPath()));
                        }).commit();
                } catch (CommitFailedException e) {
                    LOG.error("commit failed", e);
                }
                return list.toArray(new String[list.size()]);
            });
    }

    public static Supplier<String> aemQueryNodePaths(String[] aemNodes, Supplier<Long> workloadSupplier) {
        return pickFromArray(Arrays.copyOfRange(aemNodes, 0, lastNode(topLevels)), workloadSupplier);
    }

    public static Supplier<String> aemWriteNodePaths(String[] aemNodes, Supplier<Long> workloadSupplier) {
        return pickFromArray(Arrays.copyOfRange(aemNodes, firstNode(depth - bottomLevels), lastNode(depth)),
                             workloadSupplier);
    }

    public static Supplier<String> pickFromArray(String[] paths, Supplier<Long> workloadSupplier) {
        IntegerDistribution zipf = new ZipfDistribution(paths.length, 1);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction.newHasher().putLong(workload).putInt(zipfSample).hash();
            final int k = Hashing.consistentHash(hashCode, paths.length);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

    public static Supplier<String> syntheticTreeWriteNodePaths(Supplier<Long> workloadSupplier) {
        final int nBottomNodes = totalNodes(depth - bottomLevels, depth);
        IntegerDistribution zipf = new ZipfDistribution(nBottomNodes, 1);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction.newHasher().putLong(workload).putInt(zipfSample).hash();
            final int k = Hashing.consistentHash(hashCode, nBottomNodes) + firstNode(depth - bottomLevels);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

    // public static Supplier<Function<Integer, String>>
    // syntheticTreeWriteNodePathsDistributed(Supplier<Long> workloadSupplier) {
    // final int nBottomNodes = totalNodes(depth - bottomLevels, depth);
    // IntegerDistribution zipf = new ZipfDistribution(nBottomNodes, 1);
    // HashFunction hashFunction = Hashing.murmur3_32();
    //
    // return () -> {
    // final long workload = workloadSupplier.get();
    // final int zipfSample = zipf.sample();
    // final HashCode hashCode = hashFunction.newHasher()
    // .putLong(workload)
    // .putInt(zipfSample)
    // .hash();
    // final int hash = Hashing.consistentHash(hashCode, nBottomNodes);
    //
    // return (Integer clusterNode) -> {
    // final int k = linearProber(hash, clusterNode, nClusterNodes, nBottomNodes);
    // final String relativePath = mapToPath(k);
    // return PathUtils.concat(contentRootPath, relativePath);
    // };
    // };
    // }

    public static Supplier<String> syntheticTreeQueryNodePaths(Supplier<Long> workloadSupplier) {
        final int nTopNodes = totalNodes(0, topLevels);
        IntegerDistribution zipf = new ZipfDistribution(nTopNodes, 1);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction.newHasher().putLong(workload).putInt(zipfSample).hash();
            final int k = Hashing.consistentHash(hashCode, nTopNodes);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }
}
