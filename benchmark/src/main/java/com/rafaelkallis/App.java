package com.rafaelkallis;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.rafaelkallis.shared.ClusterNode;
import com.rafaelkallis.shared.Consumer;
import com.rafaelkallis.shared.Utils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.math3.distribution.IntegerDistribution;
import org.apache.commons.math3.distribution.ZipfDistribution;
import org.apache.commons.math3.stat.descriptive.moment.Mean;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.rafaelkallis.shared.TraverseUtils.*;
import static com.rafaelkallis.shared.Utils.*;

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
    static final int clusterId = Integer.getInteger("clusterId", 2);
    static final int volatilityThreshold = Integer.getInteger("volatilityThreshold", 5);
    static final int slidingWindowLength = Integer.getInteger("slidingWindowLength", 30 * 1000);

    static final long warmUpMillis = Long.getLong("warmUpMillis", 0);
    static final long warmUpTicks = Long.getLong("warmUpTicks", 0);

    static final int writesPerTick = Integer.getInteger("writesPerTick", 10);
    static final int queriesPerTick = Integer.getInteger("queriesPerTick", 1);

    static final long experimentMillis = Long.getLong("experimentMillis", 5 * 60 * 1000);
    static final long workloadMillis = Long.getLong("workloadMillis", 30 * 1000);
    static final long experimentTicks = Long.getLong("experimentTicks", 500);
    static final long workloadTicks = Long.getLong("workloadTicks", 100);

    static final String mongoUri = System.getProperty("mongoUri", "mongodb://localhost");
    static final String mongoName = System.getProperty("mongoName", "oak");
    static final String contentRootPath = System.getProperty("contentRootPath", "/content");

    static final boolean skipIndexBenchmark = Boolean.getBoolean("skipIndexBenchmark");
    //    static final String benchmark = System.getProperty("benchmark", "none");
//    static final boolean benchmarkQueryTimes = benchmark.equals("query_times");
//    static final boolean benchmarkWriteTimes = benchmark.equals("write_times");
//    static final boolean benchmarkUnproductiveNodes = benchmark.equals("unproductive_nodes");
//    static final boolean benchmarkQueryUnproductiveNodes = benchmark.equals("query_unproductive_nodes");
//    static final boolean benchmarkIndexNodes = benchmark.equals("index_nodes");
//    static final boolean benchmarkTicks = benchmark.equals("ticks");
//    static final boolean benchmarkVolatileNodes = benchmark.equals("volatile_nodes");
//    static final boolean benchmarkGCMillis = benchmark.equals("benchmark_gc_millis");
//    static final boolean benchmarkGCCleanedNodes = benchmark.equals("benchmark_gc_cleaned_nodes");
//    static final boolean benchmarkQTPCleanedNodes = benchmark.equals("benchmark_qtp_cleaned_nodes");
    static final Boolean skipInitialize = Boolean.getBoolean("skipInitialize");
    //    static final String writeMode = System.getProperty("writeMode", "toggle");
//    static final Boolean toggleWriteMode = writeMode.equals("toggle");
//    static final String queryMode = System.getProperty("queryMode", "external");
//    static final boolean nativeQueryMode = queryMode.equals("native");
//    static final boolean externalQueryMode = queryMode.equals("external");
    static final String dataset = System.getProperty("dataset", "synthetic");
    static final boolean syntheticDataset = dataset.equals("synthetic");
    static final boolean aemDataset = dataset.equals("aem");
    static final String outFileName = System.getProperty("outFileName", String.format("output_%d_%s_tau%d_L%d", System.currentTimeMillis() / 1000L, dataset, volatilityThreshold, slidingWindowLength));
    static final boolean QTP = Boolean.getBoolean("QTP");
    static final boolean GC = Boolean.getBoolean("GC");
    static final long GCPeriodicity = Long.getLong("GCPeriodicity", 30 * 1000);

    static final String workloadMode = System.getProperty("workloadMode", "tick");
    static final boolean tickMode = workloadMode.equals("tick");
    static final boolean millisMode = workloadMode.equals("millis");
    static final boolean noMode = workloadMode.equals("none");

    public static void main(String[] args) throws CommitFailedException, IOException, ParseException {

        LOG.debug("workloadMode: {}", workloadMode);
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
//        LOG.debug("benchmark: {}", benchmark);
        LOG.debug("outFileName: {}", outFileName);
        LOG.debug("skipInitialize: {}", skipInitialize);
//        LOG.debug("queryMode: {}", queryMode);
        LOG.debug("dataset: {}", dataset);
        LOG.debug("GC: {}", GC);
        LOG.debug("GCPeriodicity: {}", GCPeriodicity);
        LOG.debug("QTP: {}", QTP);

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
                    final CSVPrinter tickData = new CSVPrinter(new FileWriter(outFileName), CSVFormat.DEFAULT)
            ) {
                List<String> headers = new LinkedList<>();
                headers.add("tick");
                headers.add("traversed_index_nodes");
                headers.add("traversed_volatile_nodes");
                headers.add("traversed_unproductive_nodes");
                headers.add("index_nodes");
                headers.add("volatile_nodes");
                headers.add("unproductive_nodes");
                tickData.printRecord(headers);

                final ClusterNode clusterNode = new ClusterNode(nodeStore);

                final Timer gcTimer = new Timer();

                final Supplier<String[]> aemNodesSupplier = aemNodes(clusterNode);

                final AtomicLong warmUpTickCounter = new AtomicLong();
                final AtomicLong experimentTickCounter = new AtomicLong();

                final Supplier<Long> workloadSupplier = tickMode
                        ? tickAwareWorkload(experimentTickCounter::get)
                        : millisMode
                        ? millisAwareWorkload()
                        : noMode
                        ? noWorkload()
                        : throwException("invalid workload supplier defined");

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

                final Supplier<Long> millisSinceWarmUpStart = millisSinceNow();

                if (GC) {
                    gcTimer.schedule(
                            gcTimerTask(
                                    nodeStore,
                                    "/oak:index/pub/:index/pub",
                                    Consumer.Two.noOp()
                            ),
                            0,
                            GCPeriodicity
                    );
                }

                LOG.debug("warm-up starting");
                if (millisMode) {
                    LOG.debug("warm-up finishing at {}", Instant.now().plusMillis(warmUpMillis));
                }

                final Runnable warmUpTick = warmUpTickFactory(clusterNode, nodeStore, writeNodePaths, queryNodePaths);

                final Supplier<Boolean> warmUpLoopCondition = millisMode
                        ? () -> millisSinceWarmUpStart.get() < warmUpMillis
                        : tickMode
                        ? () -> warmUpTickCounter.get() < warmUpTicks
                        : throwException("no warmup loop condition provider");

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

//                final Consumer.One<String> dataLogger = dataLoggerFactory(out, millisMode
//                        ? millisSinceExperimentStart
//                        : tickMode
//                        ? experimentTickCounter::get
//                        : throwException("no time supplier")
//                );

                if (GC) {
                    gcTimer.cancel();
                    gcTimer.schedule(
                            gcTimerTask(
                                    nodeStore,
                                    "/oak:index/pub/:index/pub",
                                    (Long delta, Long nCleanedNodes) -> {
//                                        if (benchmarkGCMillis) {
//                                            dataLogger.accept(delta.toString());
//                                        }
//                                        if (benchmarkGCCleanedNodes) {
//                                            dataLogger.accept(nCleanedNodes.toString());
//                                        }
                                    }
                            ),
                            0,
                            GCPeriodicity
                    );
                }

                final Runnable experimentTick = experimentTickFactory(
                        clusterNode,
                        nodeStore,
                        writeNodePaths,
                        queryNodePaths,
                        (queryRuntime, travIndexNodex, travVolatileNodes, travUnproductiveNodes, indexNodes, volatileNodes, unproductiveNodes) -> {
                            try {
                                tickData.printRecord(queryRuntime, travIndexNodex, travVolatileNodes, travUnproductiveNodes, indexNodes, volatileNodes, unproductiveNodes);
                            } catch (IOException e) {
                                LOG.error("io exception", e);
                            }
                        }
                );

                final Supplier<Boolean> experimentLoopCondition = millisMode
                        ? () -> millisSinceExperimentStart.get() < experimentMillis
                        : tickMode
                        ? () -> experimentTickCounter.get() < experimentTicks
                        : throwException("no experiment loop condition supplier");

                while (experimentLoopCondition.get()) {
                    experimentTick.run();
                    final long currentTick = experimentTickCounter.incrementAndGet();
                    if (tickMode && currentTick % 50 == 0) {
                        LOG.debug("{}/{} ticks complete", currentTick, experimentTicks);
                    }
                }

                LOG.debug("experiment finished");
                gcTimer.cancel();
            }
        }
    }

    public static Runnable warmUpTickFactory(
            final ClusterNode clusterNode,
            final DocumentNodeStore nodeStore,
            final Supplier<String> writeNodePaths,
            final Supplier<String> queryNodePaths
    ) {

        final Consumer.One<String> writeOp = toggleProperty(clusterNode);
        final Function<String, Set<String>> queryOp = externalQuery(nodeStore, Consumer.Four.noOp());

        return tickFactory(
                writeNodePaths,
                writeOp,
                writesPerTick,
                queryNodePaths,
                queryOp,
                queriesPerTick,
                Consumer.Zero.noOp()
        );
    }

    public static Runnable experimentTickFactory(
            final ClusterNode clusterNode,
            final DocumentNodeStore nodeStore,
            final Supplier<String> writeNodePaths,
            final Supplier<String> queryNodePaths,
            final Consumer.Seven<Double, Double, Double, Double, Long, Long, Long> hook
    ) {
        final AtomicReference<Mean> meanQueryRuntime = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedIndexNodes = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedVolatileIndexNodes = new AtomicReference<>(new Mean());
        final AtomicReference<Mean> meanTraversedUnproductiveIndexNodes = new AtomicReference<>(new Mean());

        final Consumer.One<String> writeOp = toggleProperty(clusterNode);

        final Function<String, Set<String>> queryOp = externalQuery(
                nodeStore,
                (queryRuntime, traversedIndexNodes, traversedVolatileNodes, traversedUnproductiveNodes) -> {
                    meanQueryRuntime.get().increment(queryRuntime);
                    meanTraversedIndexNodes.get().increment(traversedIndexNodes);
                    meanTraversedVolatileIndexNodes.get().increment(traversedVolatileNodes);
                    meanTraversedUnproductiveIndexNodes.get().increment(traversedUnproductiveNodes);
                });

        return tickFactory(
                writeNodePaths,
                writeOp,
                writesPerTick,
                queryNodePaths,
                queryOp,
                queriesPerTick,
                () -> {
                    if (skipIndexBenchmark) {
                        hook.accept(
                                meanQueryRuntime.get().getResult(),
                                meanTraversedIndexNodes.get().getResult(),
                                meanTraversedVolatileIndexNodes.get().getResult(),
                                meanTraversedUnproductiveIndexNodes.get().getResult(),
                                null,
                                null,
                                null
                        );
                    } else {
                        final AtomicLong indexNodes = new AtomicLong();
                        final AtomicLong volatileNodes = new AtomicLong();
                        final AtomicLong unproductiveNodes = new AtomicLong();
                        Accumulate(
                                nodeStore,
                                "/oak:index/pub/:index/now",
                                (DocumentNodeState node, String path, Iterable<Boolean> accumulator) -> {
                                    final boolean isMatching = node.getBoolean("match");
                                    final boolean isVolatile = node.isVolatile();
                                    boolean isUnproductive = !isMatching && !isVolatile;
                                    for (boolean isChildUnproductive : accumulator) {
                                        isUnproductive &= isChildUnproductive;
                                    }

                                    indexNodes.getAndIncrement();
                                    if (isVolatile) {
                                        volatileNodes.getAndIncrement();
                                    } else if (isUnproductive) {
                                        unproductiveNodes.getAndIncrement();
                                    }
                                    return isUnproductive;
                                });

                        hook.accept(
                                meanQueryRuntime.get().getResult(),
                                meanTraversedIndexNodes.get().getResult(),
                                meanTraversedVolatileIndexNodes.get().getResult(),
                                meanTraversedUnproductiveIndexNodes.get().getResult(),
                                indexNodes.get(),
                                volatileNodes.get(),
                                unproductiveNodes.get()
                        );
                    }
                }
        );
    }

    public static Runnable tickFactory(
            Supplier<String> writeNodePaths,
            Consumer.One<String> writeOp,
            final int writesPerTick,
            Supplier<String> queryNodePaths,
            Function<String, Set<String>> queryOp,
            final int queriesPerTick,
            Consumer.Zero hook
    ) {
        return () -> {
            final Set<String> q = new HashSet<>();
            for (int i = 0; i < writesPerTick; i++) {
                final String writeNodePath = writeNodePaths.get();
                writeOp.accept(writeNodePath);

                if (q.contains(writeNodePath)) {
                    q.remove(writeNodePath);
                } else {
                    q.add(writeNodePath);
                }
            }

            // drain pending updates
            for (String writeNodePath : q) {
                writeOp.accept(writeNodePath);
            }

            // query
            for (int i = 0; i < queriesPerTick; i++) {
                queryOp.apply(queryNodePaths.get());
            }
            hook.accept();
        };
    }

    public static Consumer.One<String> toggleProperty(final ClusterNode clusterNode) {
        return (path) -> {
            try {
                clusterNode.transaction(root -> {
                    Utils.toggleProperty(root.getTree(path));
                }).commit();
            } catch (CommitFailedException e) {
                LOG.error("commit failed", e);
            }
        };
    }

//    public static Function<String, Set<String>> nativeQuery(
//            final ClusterNode o,
//            final BiConsumer<Long, Long> hook
//    ) {
//        return (String path) -> {
//            final Supplier<Long> delta = millisSinceNow();
//            final Set<String> resultSet = new HashSet<>();
//            try {
//                o.transaction(root -> {
//                    try {
//                        for (ResultRow resultRow : root.getQueryEngine().executeQuery(
//                                path + "/*[@pub='now']",
//                                Query.XPATH,
//                                Collections.emptyMap(),
//                                Collections.emptyMap()
//                        ).getRows()) {
//                            resultSet.add(resultRow.getPath());
//                        }
//                    } catch (ParseException e) {
//                        LOG.error("parse exception", e);
//                    }
//                }).commit();
//            } catch (CommitFailedException e) {
//                LOG.error("commit failed", e);
//            }
//            hook.accept(delta.get(), null);
//            return resultSet;
//        };
//    }

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
            final Consumer.Four<Long, Long, Long, Long> hook
    ) {
        final String parentPath = "/oak:index/pub/:index/now";
        return (String contentPath) -> {
            final Set<String> resultSet = new HashSet<>();
            final Supplier<Long> runtime = millisSinceNow();
            final AtomicLong traversedIndexNodes = new AtomicLong();
            final AtomicLong traversedVolatileNodes = new AtomicLong();
            final AtomicLong traversedUnproductiveNodes = new AtomicLong();
            Accumulate(
                    documentNodeStore,
                    PathUtils.concat(parentPath, PathUtils.relativize("/", contentPath)),
                    (DocumentNodeState node, String path, Iterable<Boolean> accumulator) -> {
                        final boolean isMatching = node.getBoolean("match");
                        final boolean isVolatile = node.isVolatile();
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
            hook.accept(runtime.get(), traversedIndexNodes.get(), traversedVolatileNodes.get(), traversedUnproductiveNodes.get());
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
    public static TimerTask gcTimerTask(
            DocumentNodeStore nodeStore,
            String absPath,
            Consumer.Two<Long, Long> hook
    ) {
        return new TimerTask() {
            @Override
            public void run() {
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
            }
        };
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

    public static Supplier<String> aemQueryNodePaths(String[] aemNodes, Supplier<Long> workloadSupplier) {
        return pickFromArray(
                Arrays.copyOfRange(
                        aemNodes,
                        0,
                        lastNode(topLevels)
                ),
                workloadSupplier
        );
    }

    public static Supplier<String> aemWriteNodePaths(String[] aemNodes, Supplier<Long> workloadSupplier) {
        return pickFromArray(
                Arrays.copyOfRange(
                        aemNodes,
                        firstNode(depth - bottomLevels),
                        lastNode(depth)
                ),
                workloadSupplier
        );
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
            final int k = Hashing.consistentHash(hashCode, nBottomNodes) + firstNode(depth - bottomLevels);
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
}
