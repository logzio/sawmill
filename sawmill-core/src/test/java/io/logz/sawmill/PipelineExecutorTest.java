package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class PipelineExecutorTest {
    public static final long THRESHOLD_TIME_MS = 1000;

    public PipelineExecutor pipelineExecutor;
    public List<Doc> overtimeProcessingDocs;
    public PipelineExecutionMetricsMBean pipelineExecutorMetrics;

    @Before
    public void init() {
        overtimeProcessingDocs = new ArrayList<>();
        pipelineExecutorMetrics = new PipelineExecutionMetricsMBean();
        PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(THRESHOLD_TIME_MS, pipelineExecutorMetrics,
                context -> {
                    overtimeProcessingDocs.add(context.getDoc());
                });
        pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
    }

    @Test
    public void testPipelineLongProcessingExecution() throws InterruptedException {
        Pipeline pipeline = createPipeline(false, createSleepProcessor(1100));
        Doc doc = createDoc("id", "long", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecution() {
        Pipeline pipeline = createPipeline(false, createAddFieldProcessor("newField", "Hello"));
        Doc doc = createDoc("id", "add", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertNotNull(doc.getSource().get("newField"));
        assertThat(doc.getSource().get("newField")).isEqualTo("Hello");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionWithOnErrorProcessors() {
        Pipeline pipeline = createPipeline(createExecutionStep(createFailAlwaysProcessor(),
                Arrays.asList(createOnFailureExecutionStep(createAddFieldProcessor("newField", "Hello"), "addField2"))));
        Doc doc = createDoc("id", "add", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertNotNull(doc.getSource().get("newField"));
        assertThat(doc.getSource().get("newField")).isEqualTo("Hello");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    private OnFailureExecutionStep createOnFailureExecutionStep(Processor processor, String name) {
        return new OnFailureExecutionStep(name, processor);
    }

    private ExecutionStep createExecutionStep(Processor processor, List<OnFailureExecutionStep> onFailureExecutionSteps) {
        return new ExecutionStep("fail1", processor, onFailureExecutionSteps);
    }

    @Test
    public void testPipelineExecutionFailure() {
        Pipeline pipeline = createPipeline(false, createFailAlwaysProcessor());
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isFalse();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionIgnoreFailure() {
        Processor failAlwaysProcessor = createFailAlwaysProcessor();
        Pipeline pipeline = createPipeline(true, failAlwaysProcessor);
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getProcessingFailedCount(failAlwaysProcessor.toString() + "1")).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionUnexpectedFailure() {
        Pipeline pipeline = createPipeline(false, createUnexpectedFailAlwaysProcessor());
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThatThrownBy(() -> pipelineExecutor.execute(pipeline, doc)).isInstanceOf(PipelineExecutionException.class);
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedOnUnexpectedError()).isEqualTo(1);
    }

    private Pipeline createPipeline(ExecutionStep... steps) {
        String id = "abc";
        String name = "test";
        String description = "test";
        return new Pipeline(id,
                name,
                description,
                Arrays.asList(steps),
                false);
    }

    private Pipeline createPipeline(boolean ignoreFailure, Processor... processors) {
        String id = "abc";
        String name = "test";
        String description = "test";
        List<ExecutionStep> executionSteps = (List<ExecutionStep>) Arrays.asList(processors).stream()
                .map(processor -> new ExecutionStep(processor.toString() + "1", processor, null))
                .collect(Collectors.toList());
        return new Pipeline(id,
                name,
                description,
                executionSteps,
                ignoreFailure);
    }

    private Processor createSleepProcessor(long millis) {
        return (Doc doc) -> {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {

                }
                return ProcessResult.success();
            };
    }

    private Processor createAddFieldProcessor(String k, String v) {
        return (Doc doc) -> {
                doc.addField(k, v);
                return ProcessResult.success();
            };
    }

    private Processor createUnexpectedFailAlwaysProcessor() {
        return (Doc doc) -> {
                throw new RuntimeException("test failure");
            };
    }

    private Processor createFailAlwaysProcessor() {
        return (Doc doc) -> ProcessResult.failure("test failure");
    }
}
