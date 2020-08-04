package io.logz.sawmill.benchmark;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.ExecutionStep;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.ProcessorExecutionStep;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.processors.RemoveFieldProcessor;
import io.logz.sawmill.processors.RenameFieldProcessor;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


/**
 * JMH based micro-benchmark suite used to compare pipeline configurations.
 * Use this as a baseline for comparing the performance of pipelines and specific processors.
 * The benchmark can be launched from the main() method in this class.
 */
@State(Scope.Thread)
public class SawmillMicroBenchmark {
    private static final int NUMBER_OF_FIELDS = 100;

    private Map<String, String> documentTemplate;
    private PipelineExecutor pipelineExecutor;
    private Pipeline pipelineWithTwoRenameProcessors;
    private Pipeline pipelineWithOneRename;
    private Pipeline pipelineWithTwoRenamesInOneProcessor;
    private Pipeline pipelineWithVariableRenamesInOneProcessor;
    private Pipeline pipelineWithVariableRenameProcessors;
    private Pipeline pipelineWithOneRemoveProcessor;
    private Pipeline pipelineWithVariableRemoveProcessors;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SawmillMicroBenchmark.class.getSimpleName())
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    @Setup
    public void setup() {
        pipelineExecutor = new PipelineExecutor();
        TemplateService templateService = new TemplateService();

        Map<Template, Template> renamesWithSingleRename1 = ImmutableMap.of(
                templateService.createTemplate("FieldFrom1"),
                templateService.createTemplate("FieldTo1"));

        Map<Template, Template> renamesWithSingleRename2 = ImmutableMap.of(
                templateService.createTemplate("FieldFrom2"),
                templateService.createTemplate("FieldTo2"));

        Map<Template, Template> renamesWithTwoRenames = new HashMap<Template, Template>() {{
            putAll(renamesWithSingleRename1);
            putAll(renamesWithSingleRename2);
        }};

        pipelineWithOneRename = createPipeline(
                new ProcessorExecutionStep("rename1",
                        new RenameFieldProcessor(renamesWithSingleRename1)
                )
        );

        pipelineWithTwoRenameProcessors = createPipeline(
                new ProcessorExecutionStep("rename1",
                        new RenameFieldProcessor(renamesWithSingleRename1)
                ),
                new ProcessorExecutionStep("rename2",
                        new RenameFieldProcessor(renamesWithSingleRename2)
                )
        );

        pipelineWithTwoRenamesInOneProcessor = createPipeline(
                new ProcessorExecutionStep("rename1",
                        new RenameFieldProcessor(renamesWithTwoRenames)
                )
        );


        pipelineWithVariableRenamesInOneProcessor = createPipeline(
                new ProcessorExecutionStep("rename1",
                        createRenameProcessor(0, NUMBER_OF_FIELDS, templateService)
                )
        );

        pipelineWithVariableRenameProcessors =
                createPipeline(
                        IntStream.range(0, NUMBER_OF_FIELDS).boxed().map(
                                i -> new ProcessorExecutionStep(
                                        "rename" + i,
                                        createRenameProcessor(i, 1, templateService)
                                )
                        ).collect(Collectors.toList()).toArray(new ExecutionStep[0])
                );

        pipelineWithOneRemoveProcessor = createPipeline(
                new ProcessorExecutionStep("drop1",
                        new RemoveFieldProcessor(Arrays.asList(templateService.createTemplate("FieldFrom1"))
                        )
                )
        );

        pipelineWithVariableRemoveProcessors =
                createPipeline(
                        IntStream.range(0, NUMBER_OF_FIELDS).boxed().map(
                                i -> new ProcessorExecutionStep("drop" + i,
                                        new RemoveFieldProcessor(
                                                Arrays.asList(templateService.createTemplate("FieldFrom" + i))
                                        )
                                )
                        ).collect(Collectors.toList()).toArray(new ExecutionStep[0])
                );


        documentTemplate = IntStream.range(0, NUMBER_OF_FIELDS).boxed().collect(Collectors.toMap(i -> "FieldFrom" + i, i -> "Value" + i));

        System.out.println("Finished benchmark setup. Number of fields in doc and variable renames: " + NUMBER_OF_FIELDS);
    }

    private RenameFieldProcessor createRenameProcessor(int from, int howmany, TemplateService templateService) {
        return new RenameFieldProcessor(
                IntStream.range(from, from + howmany).boxed().collect(Collectors.toMap(
                        i -> templateService.createTemplate("FieldFrom" + i),
                        i -> templateService.createTemplate("FieldTo" + i)
                ))
        );
    }

    @Benchmark
    public void benchmarkPipelineWithOneRename() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithOneRename, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithOneRemove() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithOneRemoveProcessor, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithTwoRenameProcessors() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithTwoRenameProcessors, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithTwoRenamesInOneProcessor() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithTwoRenamesInOneProcessor, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithVariableRenamesInOneProcessor() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithVariableRenamesInOneProcessor, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithVariableRenameProcessors() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithVariableRenameProcessors, doc);
    }

    @Benchmark
    public void benchmarkPipelineWithVariableRemoveProcessors() {
        Doc doc = createDocFromTemplate(documentTemplate);
        ExecutionResult executionResult = pipelineExecutor.execute(pipelineWithVariableRemoveProcessors, doc);
    }

    private Pipeline createPipeline(ExecutionStep... steps) {
        return createPipeline(false, steps);
    }

    private Pipeline createPipeline(boolean stopOnFailure, ExecutionStep... steps) {
        String id = "abc";
        return new Pipeline(id, Arrays.asList(steps), stopOnFailure);
    }

    public Doc createDoc(Object... objects) {
        if (objects.length % 2 != 0) {
            throw new RuntimeException("Can't construct map out of uneven number of elements");
        }

        LinkedHashMap map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put(objects[i], objects[++i]);
            }
        }

        return new Doc(map);
    }

    public Doc createDocFromTemplate(Map<String, String> original) {
        return new Doc(new LinkedHashMap<>(original));
    }
}
