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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class SawmillBenchmark {

    public static final int MAX_FIELDS_AMOUNT = 20;

    @Param({"id : 1, name : pipeline1, description : desc, processors: [{name:addField,config.path:benchTest, config.value:value}]"})
    public String pipelineConfig;

    @Param({"/Users/ori/Documents/logs"})
    public String docsPath;

    @Param({"1000"})
    public static long thresholdTimeMs;

    @Param({"1000"})
    public int docsAmount;

    public static Pipeline pipeline;
    public static Random random;
    public static File[] docsFilesJson;

    @Setup
    public void setup(BenchmarkParams params) {
        random = new Random();
        generateDocs();
        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory pipelineFactory = new Pipeline.Factory(processorFactoryRegistry);
        pipeline = pipelineFactory.create(pipelineConfig);
        File dir = new File(docsPath);
        docsFilesJson = dir.listFiles();
    }

    private void generateDocs() {
        for (int i=0; i< docsAmount; i++) {
            try {
                File file = new File(docsPath + "/" + RandomStringUtils.randomAlphabetic(5) + ".json");
                Map<String,Object> map = new HashMap<>();
                generateMap(map, random.nextInt(MAX_FIELDS_AMOUNT), 0);
                String data = JsonUtils.toJsonString(map);
                FileUtils.writeStringToFile(file, data, "UTF-8");
            } catch (IOException e) {
                throw new RuntimeException("failed to generate docs", e);
            }
        }

    }

    private void generateMap(Map<String,Object> map, int size, int deepLevel) {
        for (int i=0; i < size; i++) {
            String fieldName = RandomStringUtils.randomAlphabetic(5);
            if (deepLevel < 3 && random.nextBoolean()) {
                Map<String,Object> nestedMap = new HashMap<>();
                generateMap(nestedMap, random.nextInt(MAX_FIELDS_AMOUNT / 2), ++deepLevel);
                map.put(fieldName, nestedMap);
            } else if (random.nextBoolean()) {
                map.put(fieldName, generateList(random.nextInt(MAX_FIELDS_AMOUNT / 2)));
            } else {
                map.put(fieldName, RandomStringUtils.randomAlphanumeric(5));
            }
        }
    }

    private List<String> generateList(int size) {
        List<String> list = new ArrayList<>(size);
        for (int i=0; i < size; i++) {
            list.add(RandomStringUtils.randomAlphanumeric(5));
        }

        return list;
    }

    @State(Scope.Thread)
    public static class ExecutorState {
        public PipelineExecutor pipelineExecutor;
        public PipelineExecutionMetricsTracker pipelineExecutorMetrics;
        public Doc doc;

        @Setup()
        public void setup() {
            pipelineExecutorMetrics = new PipelineExecutionMetricsMBean();
            PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(thresholdTimeMs, pipelineExecutorMetrics,
                    context -> {

                    });
            pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
            doc = getRandomDoc();
        }

        private Doc getRandomDoc() {
            Doc doc;
            try {
                File file = docsFilesJson[random.nextInt(docsFilesJson.length)];
                String jsonString = FileUtils.readFileToString(file, "UTF-8");
                doc = new Doc(JsonUtils.fromJsonString(Map.class, jsonString));
            } catch (Exception e) {
                throw new RuntimeException("failed to get random doc", e);
            }

            return doc;
        }
    }

    @Benchmark
    public void testExecution(ExecutorState state) {
        state.pipelineExecutor.execute(pipeline, state.doc);
    }

    public static void main(String... args) throws Exception {
        Options opts = new OptionsBuilder()
                .include(".*")
                .warmupIterations(2)
                .measurementIterations(10)
                .jvmArgs("-server")
                .forks(1)
                .resultFormat(ResultFormatType.TEXT)
                .build();

        Collection<RunResult> records =  new Runner(opts).run();
        records.forEach(result -> {
            Result r = result.getPrimaryResult();
            System.out.println("API replied benchmark score: "
                    + r.getScore() + " "
                    + r.getScoreUnit() + " over "
                    + r.getStatistics().getN() + " iterations");
        });
    }
}
