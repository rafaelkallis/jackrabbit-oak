package com.rafaelkallis;

import static com.rafaelkallis.shared.TraverseUtils.Accumulate;
import static com.rafaelkallis.shared.Utils.firstNode;
import static com.rafaelkallis.shared.Utils.initializePropertyIndex;
import static com.rafaelkallis.shared.Utils.mapToPath;
import static com.rafaelkallis.shared.Utils.millisSinceNow;
import static com.rafaelkallis.shared.Utils.setUpCompleteTree;
import static com.rafaelkallis.shared.Utils.throwException;
import static com.rafaelkallis.shared.Utils.toggleTwice;
import static com.rafaelkallis.shared.Utils.totalNodes;
import static com.rafaelkallis.shared.Utils.generatePath;

import java.io.FileWriter;
import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.ParseException;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

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
import com.mongodb.MongoClient;
import com.rafaelkallis.shared.ClusterNode;
import com.rafaelkallis.shared.Consumer;

public class App {

    static final Logger LOG = LoggerFactory.getLogger(App.class);

    static final int templateFanoutSynthetic = Integer.getInteger("templateFanoutSynthetic", 2);
    static final int templateDepthSynthetic = Integer.getInteger("templateDepthSynthetic", 20);
    static final String templateNameSynthetic = System.getProperty("templateNameSynthetic", "oak-template-synthetic");
    static final String templateNameReal = System.getProperty("templateNameReal", "oak-template-real");

    static final int fanout = Integer.getInteger("fanout", 2);
    static final int depth = Integer.getInteger("depth", 20);
    static final int workloadSkew = Integer.getInteger("workloadSkew", 1);
    static final int topLevels = Integer.getInteger("topLevels", 1);
    static final int bottomLevels = Integer.getInteger("bottomLevels", 1);
    static final int clusterId = Integer.getInteger("clusterId", 1);
    static final int volatilityThreshold = Integer.getInteger("volatilityThreshold", 5);
    static final int slidingWindowLength = Integer.getInteger("slidingWindowLength", 30 * 1000);

    static final int updatesPerTick = Integer.getInteger("updatesPerTick", 10);
    static final int queriesPerTick = Integer.getInteger("queriesPerTick", 1);

    static final long experimentMillis = Long.getLong("experimentMillis", 5 * 60 * 1000);
    static final long workloadMillis = Long.getLong("workloadMillis", 30 * 1000);
    static final long experimentTicks = Long.getLong("experimentTicks", 2000);

    static final String mongoUri = System.getProperty("mongoUri", "mongodb://localhost");
    static final String mongoName = System.getProperty("mongoName", "oak");
    static final String contentRootPath = System.getProperty("contentRootPath", "/content");

    static final Boolean skipInitialize = Boolean.getBoolean("skipInitialize");

    static final String datasetType = System.getProperty("datasetType", "synthetic");
    static final boolean syntheticDataset = datasetType.equals("synthetic");
    static final boolean realDataset = datasetType.equals("real");
    static final String outFileName = System.getProperty("outFileName", String.format("query_output_%d_%s_tau%d_L%d", System.currentTimeMillis() / 1000L, datasetType, volatilityThreshold, slidingWindowLength));
    static final boolean QTP = Boolean.getBoolean("QTP");
    static final boolean GC = Boolean.getBoolean("GC");
    static final long GCPeriodicity = Long.getLong("GCPeriodicity", 30 * 1000);

    static final String loopMode = System.getProperty("loopMode", "millis");
    static final boolean millisLoopMode = loopMode.equals("millis");
    static final boolean tickLoopMode = loopMode.equals("tick");

    static final boolean generateSyntheticDataset = Boolean.getBoolean("generateSyntheticDataset");
    static final boolean generateRealDataset = Boolean.getBoolean("generateRealDataset");
    static final String realDatasetFile = System.getProperty("realDatasetFile", "real_dataset");
    static final String realQueryWorkloadFile = System.getProperty("realQueryWorkloadFile", "real_query_workload");
    static final String realUpdateWorkloadFile = System.getProperty("realUpdateWorkloadFile", "real_update_workload");
    static final boolean skipExperiment = Boolean.getBoolean("skipExperiment");
    static final int templateClusterId = Integer.getInteger("templateClusterId", 100);

    public static void main(String[] args) throws CommitFailedException, IOException, ParseException {

        LOG.debug("volatilityThreshold: {}", volatilityThreshold);
        LOG.debug("slidingWindowLength: {}", slidingWindowLength);
        LOG.debug("skew: {}", workloadSkew);
        LOG.debug("datasetType: {}", datasetType);
        LOG.debug("GC: {}", GC);
        LOG.debug("GCPeriodicity: {}", GCPeriodicity);
        LOG.debug("QTP: {}", QTP);

        if (generateSyntheticDataset) {
            generateSyntheticDataset();
        }

        if (generateRealDataset) {
            generateRealDataset();
        }

        if (generateSyntheticDataset || generateRealDataset || skipExperiment) {
            return;
        }

        cloneTemplate();

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

            final AtomicLong experimentTickCounter = new AtomicLong();

            final Supplier<Long> workloadSupplier = millisAwareWorkload();

            final Supplier<String> updateNodePaths = syntheticDataset
                ? syntheticUpdateNodePaths(workloadSupplier)
                : realDataset
                ? realUpdateNodePaths(workloadSupplier)
                : throwException("no update node path supplier");

            final Supplier<String> queryNodePaths = syntheticDataset
                ? syntheticQueryNodePaths(workloadSupplier)
                : realDataset
                ? realQueryNodePaths(workloadSupplier)
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
                                                                            updateNodePaths,
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
                                                           final Supplier<String> updateNodePaths,
                                                           final Supplier<String> queryNodePaths,
                                                           final Supplier<Long> experimentMillisSupplier,
                                                           final Consumer.Six<Long, Long, Double, Double, Double, Double> hook
                                                           ) {
        Mean meanQueryRuntime = new Mean();
        Mean meanTraversedIndexNodes = new Mean();
        Mean meanTraversedVolatileIndexNodes = new Mean();
        Mean meanTraversedUnproductiveIndexNodes = new Mean();

        final Consumer.One<String> updateOp = toggleTwice(clusterNode);

        final Function<String, Set<String>> queryOp = externalQuery(nodeStore,
                                                                    (queryRuntime, traversedIndexNodes, traversedVolatileNodes, traversedUnproductiveNodes) -> {
                                                                        meanQueryRuntime.increment(queryRuntime);
                                                                        meanTraversedIndexNodes.increment(traversedIndexNodes);
                                                                        meanTraversedVolatileIndexNodes.increment(traversedVolatileNodes);
                                                                        meanTraversedUnproductiveIndexNodes.increment(traversedUnproductiveNodes);
                                                                    });

        return tickFactory(updateNodePaths, updateOp, queryNodePaths, queryOp, (tick) -> {
                hook.accept(
                            tick,
                            experimentMillisSupplier.get(),
                            meanQueryRuntime.getResult(),
                            meanTraversedIndexNodes.getResult(),
                            meanTraversedVolatileIndexNodes.getResult(),
                            meanTraversedUnproductiveIndexNodes.getResult()
                            );
                meanQueryRuntime.clear();
                meanTraversedIndexNodes.clear();
                meanTraversedVolatileIndexNodes.clear();
                meanTraversedUnproductiveIndexNodes.clear();
            });
    }

    public static Consumer.One<Long> tickFactory(Supplier<String> updateNodePaths,
                                                 Consumer.One<String> updateOp,
                                                 Supplier<String> queryNodePaths,
                                                 Function<String,
                                                 Set<String>> queryOp,
                                                 Consumer.One<Long> hook) {
        return (tick) -> {
            // update
            for (int i = 0; i < updatesPerTick; i++) {
                updateOp.accept(updateNodePaths.get());
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
            hook.accept(
                        runtime.get(),
                        traversedIndexNodes.get(),
                        traversedVolatileNodes.get(),
                        traversedUnproductiveNodes.get()
                        );
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

    public static Supplier<String> realQueryNodePaths(Supplier<Long> workloadSupplier) throws IOException {
        LOG.debug("parsing paths for real dataset query workload");
        String[] paths = Files.lines(Paths.get(realQueryWorkloadFile)).toArray(String[]::new);
        return pickFromArray(paths, workloadSupplier);
    }

    public static Supplier<String> realUpdateNodePaths(Supplier<Long> workloadSupplier) throws IOException {
        LOG.debug("parsing paths for real dataset update workload");
        String[] paths = Files.lines(Paths.get(realUpdateWorkloadFile)).toArray(String[]::new);
        return pickFromArray(paths, workloadSupplier);
    }

    public static Supplier<String> pickFromArray(String[] paths, Supplier<Long> workloadSupplier) {
        IntegerDistribution zipf = new ZipfDistribution(paths.length, workloadSkew);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction
                .newHasher()
                .putLong(workload)
                .putInt(zipfSample)
                .hash();
            final int k = Hashing.consistentHash(hashCode, paths.length);
            final String relativePath = PathUtils.relativize("/", paths[k]);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

    public static Supplier<String> syntheticUpdateNodePaths(Supplier<Long> workloadSupplier) {
        final int nBottomNodes = totalNodes(depth - bottomLevels, depth);
        IntegerDistribution zipf = new ZipfDistribution(nBottomNodes, workloadSkew);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction
                .newHasher()
                .putLong(workload)
                .putInt(zipfSample)
                .hash();
            final int k = Hashing.consistentHash(hashCode, nBottomNodes) +
                firstNode(depth - bottomLevels);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

    public static Supplier<String> syntheticQueryNodePaths(Supplier<Long> workloadSupplier) {
        final int nTopNodes = totalNodes(0, topLevels);
        IntegerDistribution zipf = new ZipfDistribution(nTopNodes, workloadSkew);
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

    public static void cloneTemplate() {
        final String templateName = syntheticDataset
            ? templateNameSynthetic
            : realDataset
            ? templateNameReal
            : null;

        try (MongoClient mongoClient = new MongoClient()) {
            LOG.debug("dropping {}", mongoName);
            mongoClient.dropDatabase(mongoName);
            LOG.debug("finished");
            LOG.debug("cloning {} to {}", templateName, mongoName);
            final Bson command = BsonDocument.parse(String.format("{ copydb: 1, fromdb: \"%s\", todb: \"%s\"}", templateName, mongoName));
            mongoClient.getDatabase("admin").runCommand(command);
            LOG.debug("finished");
        }
    }

    public static void generateSyntheticDataset() throws CommitFailedException, UnknownHostException {
        LOG.debug("generating synthetic dataset");
        LOG.debug("- droppping previous template");
        try (MongoClient mongoClient = new MongoClient()) {
            mongoClient.dropDatabase(templateNameSynthetic);
        }
        try (final DocumentNodeStore nodeStore = new DocumentMK.Builder()
             .setMongoDB(mongoUri, templateNameSynthetic, 16)
             .setClusterId(templateClusterId).setVolatilityThreshold(volatilityThreshold)
             .setSlidingWindowLength(slidingWindowLength)
             .getNodeStore()) {
            ClusterNode clusterNode = new ClusterNode(nodeStore);
            clusterNode.transaction(root -> {
                    LOG.debug("- initializing property index");
                    initializePropertyIndex(root, "pub");
                    LOG.debug("- generating synthetic dataset");
                    Tree content = generatePath(root.getTree("/"), contentRootPath);
                    setUpCompleteTree(content, fanout, depth);
                    LOG.debug("- committing");
                }).commit();
        }
    }

    public static void generateRealDataset() throws IOException, CommitFailedException{
        LOG.debug("generating real dataset");
        LOG.debug("- droppping previous template");
        try (MongoClient mongoClient = new MongoClient()) {
            mongoClient.dropDatabase(templateNameReal);
        }
        try (Stream<String> lines = Files.lines(Paths.get(realDatasetFile));
             DocumentNodeStore store = new DocumentMK.Builder()
             .setMongoDB(mongoUri, templateNameReal, 16)
             .setClusterId(templateClusterId)
             .setVolatilityThreshold(volatilityThreshold)
             .setSlidingWindowLength(slidingWindowLength)
             .getNodeStore()){
            ClusterNode clusterNode = new ClusterNode(store);
            clusterNode.transaction(root -> {
                    LOG.debug("- initializing property index");
                    initializePropertyIndex(root, "pub");
                    LOG.debug("- generating real dataset");
                    Tree content = generatePath(root.getTree("/"), contentRootPath);
                    lines.forEach(path -> {
                            generatePath(content, path);
                        });
                    LOG.debug("- committing");
                }).commit();
        }
    }
}
