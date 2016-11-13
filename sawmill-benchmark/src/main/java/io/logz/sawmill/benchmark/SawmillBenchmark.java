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
import org.apache.commons.lang3.RandomStringUtils;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.json.simple.parser.JSONParser;
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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

@BenchmarkMode(Mode.Throughput)
@State(Scope.Benchmark)
public class SawmillBenchmark {

    @Param({"id : 1, name : pipeline1, description : desc, processors: [{name:addField,config.path:benchTest, config.value:value}]"})
    public String pipelineConfig;

    @Param({"/Users/ori/Documents/logs"})
    public String docsPath;

    @Param({"/Users/ori/Documents/logs/config/config.json"})
    public String jsonConfigPath;

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
        //generateDocs();
        ProcessorFactoryRegistry processorFactoryRegistry = new ProcessorFactoryRegistry();
        ProcessorFactoriesLoader.getInstance().loadAnnotatedProcesses(processorFactoryRegistry);
        Pipeline.Factory pipelineFactory = new Pipeline.Factory(processorFactoryRegistry);
        pipeline = pipelineFactory.create(pipelineConfig);
        File dir = new File(docsPath);
        docsFilesJson = dir.listFiles();
    }

    private void generateDocs() {
        JsonFactory jsonFactory = new JsonFactory();
        for (int i=0; i< docsAmount; i++) {
            try {
                Writer writer = new FileWriter(RandomStringUtils.randomAlphabetic(5) + ".json");
                JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
            } catch (IOException e) {
            }
        }

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
            File file = docsFilesJson[random.nextInt(docsFilesJson.length)];
            try (FileReader fileReader = new FileReader(file)){
                Object parse = new JSONParser().parse(fileReader);
                doc = new Doc(JsonUtils.fromJsonString(Map.class, parse.toString()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            finally {
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
