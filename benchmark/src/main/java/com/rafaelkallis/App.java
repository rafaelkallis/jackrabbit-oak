package com.rafaelkallis;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.rafaelkallis.shared.ClusterNode;
import com.rafaelkallis.shared.Utils;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeState;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.bson.BsonDocument;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.query.Query;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.rafaelkallis.shared.TraverseUtils.*;
import static com.rafaelkallis.shared.Utils.*;

public class App {

    static final Logger LOG = LoggerFactory.getLogger(App.class);

    static final String time = System.getProperty("time", "millis");
    static final boolean millisMode = time.equals("millis");
    static final boolean tickMode = time.equals("tick");
    static final int fanout = Integer.getInteger("fanout", 2);
    static final int depth = Integer.getInteger("depth", 20);
    static final int templateFanout = Integer.getInteger("templateFanout", 2);
    static final int templateDepth = Integer.getInteger("templateDepth", 20);
    static final String templateName = System.getProperty("templateName", "oak-template");
    static final int topLevels = Integer.getInteger("topLevels", 1);
    static final int bottomLevels = Integer.getInteger("bottomLevels", 1);
    static final int clusterId = Integer.getInteger("clusterId", 2);
    static final int volatilityThreshold = Integer.getInteger("volatilityThreshold", 5);
    static final long warmUpMillis = Long.getLong("warmUpMillis", 0);
    static final long warmUpTicks = Long.getLong("warmUpTicks", 0);
    static final int slidingWindowLength = Integer.getInteger("slidingWindowLength", 30 * 1000);
    static final int skew = Integer.getInteger("skew", 1);
    static final int writesPerTick = Integer.getInteger("writesPerTick", 10);
    static final int queriesPerTick = Integer.getInteger("queriesPerTick", 1);
    static final long experimentMillis = Long.getLong("experimentMillis", 5 * 60 * 1000);
    static final long workloadMillis = Long.getLong("workloadMillis", 30 * 1000);
    static final long experimentTicks = Long.getLong("experimentTicks", 500);
    static final long workloadTicks = Long.getLong("workloadTicks", 100);
    static final String mongoUri = System.getProperty("mongoUri", "mongodb://localhost");
    static final String mongoName = System.getProperty("mongoName", "oak");
    static final String contentRootPath = System.getProperty("contentRootPath", "/content");
    static final String benchmark = System.getProperty("benchmark", "none");
    static final boolean benchmarkQueryTimes = benchmark.equals("query_times");
    static final boolean benchmarkWriteTimes = benchmark.equals("write_times");
    static final boolean benchmarkUnproductiveNodes = benchmark.equals("unproductive_nodes");
    static final boolean benchmarkQueryUnproductiveNodes = benchmark.equals("query_unproductive_nodes");
    static final boolean benchmarkIndexNodes = benchmark.equals("index_nodes");
    static final boolean benchmarkTicks = benchmark.equals("ticks");
    static final Boolean skipInitialize = Boolean.getBoolean("skipInitialize");
    static final String writeMode = System.getProperty("writeMode", "toggle");
    static final Boolean toggleWriteMode = writeMode.equals("toggle");
    static final String queryMode = System.getProperty("queryMode", "external");
    static final boolean nativeQueryMode = queryMode.equals("native");
    static final boolean externalQueryMode = queryMode.equals("external");
    static final String dataset = System.getProperty("dataset", "synthetic");
    static final boolean syntheticDataset = dataset.equals("synthetic");
    static final boolean aemDataset = dataset.equals("aem");
    static final String outFileName = System.getProperty("outFileName", String.format("output_%s_%d_%s_tau%d_L%d", benchmark, System.currentTimeMillis() / 1000L, dataset, volatilityThreshold, slidingWindowLength));
    static final boolean QTP = Boolean.getBoolean("QTP");
    static final long GCPeriodicity = Long.getLong("GCPerdiodicity", Long.MAX_VALUE);

    public static void main(String[] args) throws CommitFailedException, IOException, ParseException {

        LOG.debug("time: {}", time);
        LOG.debug("fanout: {}", fanout);
        LOG.debug("depth: {}", depth);
        LOG.debug("templateFanout: {}", templateFanout);
        LOG.debug("templateDepth: {}", templateDepth);
        LOG.debug("templateName: {}", templateName);
        LOG.debug("topLevels: {}", topLevels);
        LOG.debug("bottomLevels: {}", bottomLevels);
        LOG.debug("clusterId: {}", clusterId);
        LOG.debug("volatilityThreshold: {}", volatilityThreshold);
        LOG.debug("warmUpMillis: {}", warmUpMillis);
        LOG.debug("warmUpTicks: {}", warmUpTicks);
        LOG.debug("slidingWindowLength: {}", slidingWindowLength);
        LOG.debug("skew: {}", skew);
        LOG.debug("writesPerTick: {}", writesPerTick);
        LOG.debug("queriesPerTick: {}", queriesPerTick);
        LOG.debug("experimentMillis: {}", experimentMillis);
        LOG.debug("workloadMillis: {}", workloadMillis);
        LOG.debug("experimentTicks: {}", experimentTicks);
        LOG.debug("workloadTicks: {}", workloadTicks);
        LOG.debug("mongoUri: {}", mongoUri);
        LOG.debug("mongoName: {}", mongoName);
        LOG.debug("contentRootPath: {}", contentRootPath);
        LOG.debug("benchmark: {}", benchmark);
        LOG.debug("outFileName: {}", outFileName);
        LOG.debug("skipInitialize: {}", skipInitialize);
        LOG.debug("queryMode: {}", queryMode);
        LOG.debug("dataset: {}", dataset);

        try (final MongoClient mongoClient = new MongoClient()) {
            final DB db = mongoClient.getDB(mongoName);

            if (!skipInitialize) {
                LOG.debug("dropping {}", mongoName);
                mongoClient.dropDatabase(mongoName);

                if (fanout == templateFanout && depth == templateDepth) {
                    LOG.debug("cloning {} to {}", templateName, mongoName);
                    final Bson command = BsonDocument.parse(String.format("{ copydb: 1, fromdb: \"%s\", todb: \"%s\"}", templateName, mongoName));
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

//            final String[] topNodes = ClusterNode.singleTransaction(
//                    db,
//                    root -> {
//                        return slice(root.getTree(contentRootPath), 0, topLevels);
//                    }
//            ).commit();
//
//            final String[] bottomNodes = ClusterNode.singleTransaction(
//                    db,
//                    root -> {
//                        return slice(root.getTree(contentRootPath), depth - bottomLevels, depth);
//                    }
//            ).commit();

            try (
                    final DocumentNodeStore nodeStore = new DocumentMK.Builder()
                            .setMongoDB(mongoUri, mongoName, 16)
//                            .memoryCacheSize(16 * 1024 * 1024)
                            .setClusterId(clusterId)
                            .setVolatilityThreshold(volatilityThreshold)
                            .setSlidingWindowLength(slidingWindowLength)
//                            .setPropertyIndexCleanUpProps("pub")
                            .getNodeStore();
                    final FileWriter out = new FileWriter(outFileName)
            ) {
                final ClusterNode clusterNode = new ClusterNode(nodeStore);

                final AtomicLong warmUpTickCounter = new AtomicLong(0);
                final AtomicLong experimentTickCounter = new AtomicLong(0);

                final Supplier<Long> workload = millisMode
                        ? millisAwareWorkload()
                        : tickMode
                        ? tickAwareWorkload(experimentTickCounter::get)
                        : noWorkload();

                final Supplier<String> writeNodePaths = syntheticDataset
                        ? syntheticTreeWriteNodePaths(workload)
                        : aemDataset
                        ? throwException()
                        : throwException();

                final Supplier<String> queryNodePaths = syntheticDataset
                        ? syntheticTreeQueryNodePaths(workload)
                        : aemDataset
                        ? throwException()
                        : throwException();

                final Supplier<Long> millisSinceWarmUpStart = millisSinceNow();

                LOG.debug("warm-up starting");
                if (millisMode) {
                    LOG.debug("warm-up finishing at {}", Instant.now().plusMillis(warmUpMillis));
                }

                final Runnable warmUpTick = warmUpTickFactory(clusterNode, nodeStore, writeNodePaths, queryNodePaths);

                final Supplier<Boolean> warmUpLoopCondition = millisMode
                        ? () -> millisSinceWarmUpStart.get() < warmUpMillis
                        : tickMode
                        ? () -> warmUpTickCounter.get() < warmUpTicks
                        : throwException();

                while (warmUpLoopCondition.get()) {
                    warmUpTick.run();
                    long currentTick = warmUpTickCounter.incrementAndGet();
                    if (tickMode && currentTick % 50 == 0) {
                        LOG.debug("{}/{} ticks complete", currentTick, warmUpTicks);
                    }
                }

                LOG.debug("warm-up finished");

                final Supplier<Long> millisSinceExperimentStart = millisSinceNow();

                LOG.debug("experiment starting");
                if (millisMode) {
                    LOG.debug("experiment finishing at {}", Instant.now().plusMillis(experimentMillis));
                }

                final Consumer<String> dataLogger = dataLoggerFactory(out, millisMode
                        ? millisSinceExperimentStart
                        : tickMode
                        ? experimentTickCounter::get
                        : throwException()
                );

                final Runnable experimentTick = experimentTickFactory(
                        clusterNode,
                        nodeStore,
                        writeNodePaths,
                        queryNodePaths,
                        dataLogger
                );

                final Supplier<Boolean> experimentLoopCondition = millisMode
                        ? () -> millisSinceExperimentStart.get() < experimentMillis
                        : tickMode
                        ? () -> experimentTickCounter.get() < experimentTicks
                        : throwException();

                while (experimentLoopCondition.get()) {
                    experimentTick.run();
                    final long currentTick = experimentTickCounter.incrementAndGet();
                    if (tickMode && currentTick % 50 == 0) {
                        LOG.debug("{}/{} ticks complete", currentTick, experimentTicks);
                    }
                }

                LOG.debug("experiment finished");
            }
        }
    }

    public static Consumer<String> writeOpFactory(
            final ClusterNode clusterNode,
            final Consumer<Long> hook
    ) {
        return toggleWriteMode
                ? toggleProperty(clusterNode, hook)
                : noOp();
    }

    public static Function<String, Set<String>> queryOpFactory(
            final ClusterNode o,
            final DocumentNodeStore nodeStore,
            final BiConsumer<Long, Long> hook
    ) {
        return externalQueryMode
                ? externalQuery(nodeStore, hook)
                : nativeQueryMode
                ? nativeQuery(o, hook)
                : noOp(new HashSet<>());
    }

    public static Runnable warmUpTickFactory(
            final ClusterNode clusterNode,
            final DocumentNodeStore nodeStore,
            final Supplier<String> writeNodePaths,
            final Supplier<String> queryNodePaths
    ) {

        final Consumer<String> writeOp = writeOpFactory(clusterNode, noOp());
        final Function<String, Set<String>> queryOp = queryOpFactory(clusterNode, nodeStore, noOpBi());

        return tickFactory(
                writeNodePaths,
                writeOp,
                writesPerTick,
                queryNodePaths,
                queryOp,
                queriesPerTick,
                noOp()
        );
    }

    public static Runnable experimentTickFactory(
            final ClusterNode clusterNode,
            final DocumentNodeStore nodeStore,
            final Supplier<String> writeNodePaths,
            final Supplier<String> queryNodePaths,
            final Consumer<String> dataLogger
    ) {
        final Consumer<String> writeOp = writeOpFactory(clusterNode, delta -> {
            if (benchmarkWriteTimes) {
                dataLogger.accept(delta.toString());
            }
        });

        final Function<String, Set<String>> queryOp = queryOpFactory(clusterNode, nodeStore, (delta, nUnproductiveNodes) -> {
            if (benchmarkQueryTimes) {
                dataLogger.accept(delta.toString());
            }
            if (benchmarkQueryUnproductiveNodes) {
                dataLogger.accept(nUnproductiveNodes.toString());
            }
        });

        return tickFactory(
                writeNodePaths,
                writeOp,
                writesPerTick,
                queryNodePaths,
                queryOp,
                queriesPerTick,
                delta -> {
                    if (benchmarkTicks) {
                        dataLogger.accept(delta.toString());
                    }
                    if (benchmarkUnproductiveNodes) {
                        final AtomicLong n = new AtomicLong(0L);
                        Accumulate(
                                nodeStore,
                                "/oak:index/pub/:index/now",
                                (DocumentNodeState node, String path, Iterable<Boolean> accumulator) -> {
                                    boolean isUnproductive = !node.hasProperty("match") && !node.isVolatile();
                                    for (boolean isChildUnproductive : accumulator) {
                                        isUnproductive &= isChildUnproductive;
                                    }
                                    if (isUnproductive) {
                                        n.getAndIncrement();
                                    }
                                    return isUnproductive;
                                });
                        dataLogger.accept(Long.toString(n.get()));
                    }
                    if (benchmarkIndexNodes) {
                        final AtomicLong n = new AtomicLong(0L);
                        PostOrder(
                                nodeStore,
                                "/oak:index/pub/:index/now",
                                (DocumentNodeState node, String path) -> n.getAndIncrement()
                        );
                        dataLogger.accept(Long.toString(n.get()));
                    }
                }
        );
    }

    public static Runnable tickFactory(
            Supplier<String> writeNodePaths,
            Consumer<String> writeOp,
            final int writesPerTick,
            Supplier<String> queryNodePaths,
            Function<String, Set<String>> queryOp,
            final int queriesPerTick,
            Consumer<Long> hook
    ) {
        return () -> {
            final Supplier<Long> delta = millisSinceNow();
            for (int i = 0; i < writesPerTick; i++) {
                writeOp.accept(writeNodePaths.get());
            }
            for (int i = 0; i < queriesPerTick; i++) {
                queryOp.apply(queryNodePaths.get());
            }
            hook.accept(delta.get());
        };
    }

    public static Consumer<String> toggleProperty(
            final ClusterNode clusterNode,
            final Consumer<Long> hook
    ) {
        return (path) -> {
            try {
                final Supplier<Long> delta = millisSinceNow();
                clusterNode.transaction(root -> {
                    Utils.toggleProperty(root.getTree(path));
                }).commit();
                hook.accept(delta.get());
            } catch (CommitFailedException e) {
                LOG.error("commit failed", e);
            }
        };
    }

    public static Function<String, Set<String>> nativeQuery(
            final ClusterNode o,
            final BiConsumer<Long, Long> hook
    ) {
        return (String path) -> {
            final Supplier<Long> delta = millisSinceNow();
            final Set<String> resultSet = new HashSet<>();
            try {
                o.transaction(root -> {
                    try {
                        for (ResultRow resultRow : root.getQueryEngine().executeQuery(
                                path + "/*[@pub='now']",
                                Query.XPATH,
                                Collections.emptyMap(),
                                Collections.emptyMap()
                        ).getRows()) {
                            resultSet.add(resultRow.getPath());
                        }
                    } catch (ParseException e) {
                        LOG.error("parse exception", e);
                    }
                }).commit();
            } catch (CommitFailedException e) {
                LOG.error("commit failed", e);
            }
            hook.accept(delta.get(), null);
            return resultSet;
        };
    }

    /**
     * Runs Query in external mode.
     *
     * @param documentNodeStore
     * @param hook:             A function which accepts
     *                          1) the query execution time as 1st parameter and
     *                          2) the number of unproductive nodes encountered during execution as 2nd parameter
     * @return A function which accepts a path and returns the set of content node paths which
     * 1) are descendants of the provided path and
     * 2) have property "pub" set to "now"
     */
    public static Function<String, Set<String>> externalQuery(
            final DocumentNodeStore documentNodeStore,
            final BiConsumer<Long, Long> hook
    ) {
        final String parentPath = "/oak:index/pub/:index/now";
        return (String contentPath) -> {
            final Supplier<Long> delta = millisSinceNow();
            final Set<String> resultSet = new HashSet<>();
            final AtomicLong nUnproductiveNodes = new AtomicLong();
            Accumulate(
                    documentNodeStore,
                    PathUtils.concat(parentPath, PathUtils.relativize("/", contentPath)),
                    (DocumentNodeState node, String path, Iterable<Boolean> accumulator) -> {
                        final boolean isMatching = node.getBoolean("match");
                        if (isMatching) {
                            resultSet.add(PathUtils.relativize(parentPath, path));
                        }
                        boolean isUnproductive = !isMatching && !node.isVolatile();
                        for (boolean isChildUnproductive : accumulator) {
                            isUnproductive &= isChildUnproductive;
                        }
                        if (isUnproductive) {
                            nUnproductiveNodes.getAndIncrement();

                            if (QTP) {
                                node.builder().remove();
                            }
                        }
                        return isUnproductive;
                    });
            hook.accept(delta.get(), nUnproductiveNodes.get());
            return resultSet;
        };
    }

    /**
     * Cleans all unproductive nodes under the given path
     *
     * @param nodeStore
     * @param absPath
     * @param hook:A    function which accepts
     *                  1) the cleanup execution time as 1st parameter and
     *                  2) the number of nodes cleaned during execution as 2nd parameter
     * @return
     */
    public Runnable cleanUnproductiveNodes(
            DocumentNodeStore nodeStore,
            String absPath,
            BiConsumer<Long, Long> hook
    ) {
        return () -> {
            final Supplier<Long> delta = millisSinceNow();
            final AtomicLong nCleanedNodes = new AtomicLong();
            Accumulate(nodeStore, absPath, (DocumentNodeState node, String path, Iterable<Boolean> accumulator) -> {
                boolean hasChild = false;
                for (boolean isChildRemoved : accumulator) {
                    hasChild |= !isChildRemoved;
                }
                if (!hasChild && !node.hasProperty("match") && !node.isVolatile()) {
                    nCleanedNodes.getAndIncrement();
                    node.builder().remove();
                    return true;
                }
                return false;
            });
            hook.accept(delta.get(), nCleanedNodes.get());
        };
    }

    public static <R> Consumer<R> noOp() {
        return o -> {
        };
    }

    public static <T1, T2> BiConsumer<T1, T2> noOpBi() {
        return (o1, o2) -> {
        };
    }

    public static <T, R> Function<T, R> noOp(R dummy) {
        return o -> dummy;
    }

    public static Supplier<Long> millisAwareWorkload() {
        final Supplier<Long> delta = millisSinceNow();
        return () -> delta.get() / workloadMillis;
    }

    public static Supplier<Long> tickAwareWorkload(Supplier<Long> ticks) {
        return () -> ticks.get() / workloadTicks;
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

    public static Supplier<String> pickFromArray(String[] paths, Supplier<Long> workloadSupplier) {
        IntegerDistribution zipf = new ZipfDistribution(paths.length, 1);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction.newHasher()
                    .putLong(workload)
                    .putInt(zipfSample)
                    .hash();
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
            final HashCode hashCode = hashFunction.newHasher()
                    .putLong(workload)
                    .putInt(zipfSample)
                    .hash();
            final int k = Hashing.consistentHash(hashCode, nBottomNodes);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

//    public static Supplier<Function<Integer, String>> syntheticTreeWriteNodePathsDistributed(Supplier<Long> workloadSupplier) {
//        final int nBottomNodes = totalNodes(depth - bottomLevels, depth);
//        IntegerDistribution zipf = new ZipfDistribution(nBottomNodes, 1);
//        HashFunction hashFunction = Hashing.murmur3_32();
//
//        return () -> {
//            final long workload = workloadSupplier.get();
//            final int zipfSample = zipf.sample();
//            final HashCode hashCode = hashFunction.newHasher()
//                    .putLong(workload)
//                    .putInt(zipfSample)
//                    .hash();
//            final int hash = Hashing.consistentHash(hashCode, nBottomNodes);
//
//            return (Integer clusterNode) -> {
//                final int k = linearProber(hash, clusterNode, nClusterNodes, nBottomNodes);
//                final String relativePath = mapToPath(k);
//                return PathUtils.concat(contentRootPath, relativePath);
//            };
//        };
//    }

    public static Supplier<String> syntheticTreeQueryNodePaths(Supplier<Long> workloadSupplier) {
        final int nTopNodes = totalNodes(0, topLevels);
        IntegerDistribution zipf = new ZipfDistribution(nTopNodes, 1);
        HashFunction hashFunction = Hashing.murmur3_32();

        return () -> {
            final long workload = workloadSupplier.get();
            final int zipfSample = zipf.sample();
            final HashCode hashCode = hashFunction.newHasher()
                    .putLong(workload)
                    .putInt(zipfSample)
                    .hash();
            final int k = Hashing.consistentHash(hashCode, nTopNodes);
            final String relativePath = mapToPath(k);
            return PathUtils.concat(contentRootPath, relativePath);
        };
    }

    public static Consumer<String> dataLoggerFactory(
            FileWriter fileWriter,
            Supplier<Long> experimentTime
    ) {
        return data -> {
            try {
                fileWriter.append(experimentTime.get().toString()).append(',').append(data).append('\n');
            } catch (IOException e) {
                LOG.error("io exception", e);
            }
        };
    }
}
