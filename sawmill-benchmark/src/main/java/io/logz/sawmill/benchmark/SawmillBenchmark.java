package io.logz.sawmill.benchmark;

import com.google.common.collect.Iterables;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutionMetricsMBean;
import io.logz.sawmill.PipelineExecutionMetricsTracker;
import io.logz.sawmill.PipelineExecutionTimeWatchdog;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.ProcessorFactoriesLoader;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.utilities.JsonUtils;
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

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class SawmillBenchmark {

    @Param({"id : 1, name : pipeline1, description : desc, processors: [{name:addField,config.path:benchTest, config.value:value}]"})
    public static String pipelineConfig;

    @Param({"/Users/ori/Documents/logs"})
    public static String docsPath;

    @Param({"50"})
    public static long thresholdTimeMs;

    @Param({"1000"})
    public static int docsAmount;

    @Param({"50"})
    public static int docsPerFile;

    @Param({"RANDOM"})
    public static String docType;

    public static Pipeline pipeline;
    public static PipelineExecutionMetricsTracker pipelineExecutorMetrics;
    public static File[] jsonDocFiles;
    public static AtomicInteger docIndex;
    public static PipelineExecutionTimeWatchdog watchdog;
    public static PipelineExecutor pipelineExecutor;

    @Setup
    public void setup(BenchmarkParams params) {
        DocumentGenerator.generateDocs(docsPath, docsAmount / docsPerFile, docsPerFile, DocumentGenerator.DocType.valueOf(docType));

        setupSawmill();
        setupInput();
    }

    private void setupInput() {
        docIndex = new AtomicInteger();
        File dir = new File(docsPath);
        FilenameFilter filter = (File directory, String name) -> name.matches(".*\\.json$");
        jsonDocFiles = dir.listFiles(filter);
    }

    private void setupSawmill() {
        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcessors(processorFactoryRegistry);
        pipelineExecutorMetrics = new PipelineExecutionMetricsMBean();
        watchdog = new PipelineExecutionTimeWatchdog(thresholdTimeMs, pipelineExecutorMetrics, context -> { });
        pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
        Pipeline.Factory pipelineFactory = new Pipeline.Factory(processorFactoryRegistry);
        pipeline = pipelineFactory.create(pipelineConfig);
    }

    @State(Scope.Thread)
    public static class ExecutorState {
        public Iterator<Doc> docIterator;

        @Setup()
        public void setup() {
            File file = jsonDocFiles[docIndex.getAndIncrement()];
            docIterator = extractDocs(file);
        }

        private Iterator<Doc> extractDocs(File file) {
            List<Doc> docs = new ArrayList<>();
            try {
                LineIterator lineIterator = FileUtils.lineIterator(file, "UTF-8");
                while (lineIterator.hasNext()) {
                    String line = lineIterator.next();
                    if (!line.isEmpty()) {
                        docs.add(new Doc(JsonUtils.fromJsonString(Map.class, line)));
                    }
                }

            } catch (Exception e) {
                throw new RuntimeException("failed to extract docs from file [" + file + "]", e);
            }

            return Iterables.cycle(docs).iterator();
        }
    }

    @Benchmark
    public void testExecution(ExecutorState state) {
        pipelineExecutor.execute(pipeline, state.docIterator.next());
    }
}
