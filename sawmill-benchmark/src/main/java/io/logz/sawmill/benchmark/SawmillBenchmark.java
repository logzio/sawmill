package io.logz.sawmill.benchmark;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutionMetricsMBean;
import io.logz.sawmill.PipelineExecutionMetricsTracker;
import io.logz.sawmill.PipelineExecutionTimeWatchdog;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.ProcessorFactoriesLoader;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class SawmillBenchmark {

    @Param({"id : 1, name : pipeline1, description : desc, processors: [{name:addField,config.path:benchTest, config.value:value}]"})
    public static String pipelineConfig;

    @Param({"/Users/ori/Documents/logs"})
    public static String docsPath;

    @Param({"1000"})
    public static long thresholdTimeMs;

    @Param({"1000"})
    public static int docsAmount;

    @Param({"50"})
    public static int docsPerFile;

    @Param({"RANDOM"})
    public static String docType;

    public static Pipeline pipeline;
    public static PipelineExecutionMetricsTracker pipelineExecutorMetrics;
    public static File[] docsFilesJson;
    public static AtomicInteger docIndex;
    public static PipelineExecutionTimeWatchdog watchdog;

    @Setup
    public void setup(BenchmarkParams params) {
        docIndex = new AtomicInteger();
        DocumentGenerator.generateDocs(docsPath, docsAmount / docsPerFile, docsPerFile, DocumentGenerator.DocType.valueOf(docType));
        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        pipelineExecutorMetrics = new PipelineExecutionMetricsMBean();
        watchdog = new PipelineExecutionTimeWatchdog(thresholdTimeMs, pipelineExecutorMetrics,
                context -> {

                });
        Pipeline.Factory pipelineFactory = new Pipeline.Factory(processorFactoryRegistry);
        pipeline = pipelineFactory.create(pipelineConfig);
        File dir = new File(docsPath);
        FilenameFilter filter = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                if (name.matches(".*\\.json$")) return true;
                return false;
            }
        };
        docsFilesJson = dir.listFiles(filter);
    }

    @State(Scope.Thread)
    public static class ExecutorState {
        public PipelineExecutor pipelineExecutor;
        public Queue<Doc> docQueue;

        @Setup()
        public void setup() {
            pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
            docQueue = getDocs();
        }

        private Queue<Doc> getDocs() {
            Queue<Doc> queue = new CircularFifoQueue<>(docsPerFile);
            try {
                File file = docsFilesJson[docIndex.getAndIncrement()];
                LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
                while (lineIterator.hasNext()) {
                    String line = lineIterator.next();
                    if (!line.isEmpty()) {
                        queue.add(new Doc(JsonUtils.fromJsonString(Map.class, line)));
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException("failed to get random doc", e);
            }

            return queue;
        }
    }

    @Benchmark
    public void testExecution(ExecutorState state) {
        Doc doc = state.docQueue.peek();
        state.pipelineExecutor.execute(pipeline, doc);
        state.docQueue.add(doc);
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(".*")
                .warmupIterations(2)
                .measurementIterations(10)
                .jvmArgs("-server")
                .forks(1)
                .threads(8)
                .resultFormat(ResultFormatType.JSON)
                .build();

        new Runner(opts).run();
    }
}
