package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

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
        Pipeline pipeline = createPipeline(false, createSleepExecutionStep(1100));
        Doc doc = createDoc("id", "long", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecution() {
        Pipeline pipeline = createPipeline(false, createAddFieldExecutionStep("newField", "Hello"));
        Doc doc = createDoc("id", "add", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertNotNull(doc.getSource().get("newField"));
        assertThat(doc.getSource().get("newField")).isEqualTo("Hello");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionWithOnErrorProcessors() {
        Pipeline pipeline = createPipeline(createFailAlwaysExecutionStep(Arrays.asList(createOnFailureExecutionStep(createAddFieldProcessor("newField", "Hello"), "addField2"))));
        Doc doc = createDoc("id", "add", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertNotNull(doc.getSource().get("newField"));
        assertThat(doc.getSource().get("newField")).isEqualTo("Hello");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionFailure() {
        Pipeline pipeline = createPipeline(false, createFailAlwaysExecutionStep(null));
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isFalse();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionIgnoreFailure() {
        Pipeline pipeline = createPipeline(true, createFailAlwaysExecutionStep(null));
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getProcessingFailedCount("fail1")).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionUnexpectedFailure() {
        Pipeline pipeline = createPipeline(false, createUnexpectedFailAlwaysExecutionStep());
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThatThrownBy(() -> pipelineExecutor.execute(pipeline, doc)).isInstanceOf(PipelineExecutionException.class);
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedOnUnexpectedError()).isEqualTo(1);
    }

    private Pipeline createPipeline(ExecutionStep... steps) {
        return createPipeline(false, steps);
    }

    private Pipeline createPipeline(boolean ignoreFailure, ExecutionStep... steps) {
        String id = "abc";
        String name = "test";
        String description = "test";
        return new Pipeline(id,
                name,
                description,
                Arrays.asList(steps),
                ignoreFailure);
    }

    private OnFailureExecutionStep createOnFailureExecutionStep(Processor processor, String name) {
        return new OnFailureExecutionStep(name, processor);
    }

    private ExecutionStep createSleepExecutionStep(long millis) {
        return new ExecutionStep("sleep1", (Doc doc) -> {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {

                }
                return ProcessResult.success();
            });
    }

    private ExecutionStep createAddFieldExecutionStep(String k, String v) {
        return new ExecutionStep("add1", createAddFieldProcessor(k, v));
    }

    private Processor createAddFieldProcessor(String k, String v) {
        return (Doc doc) -> {
                doc.addField(k, v);
                return ProcessResult.success();
            };
    }

    private ExecutionStep createUnexpectedFailAlwaysExecutionStep() {
        return new ExecutionStep("failHard1", (Doc doc) -> {
                throw new RuntimeException("test failure");
            });
    }

    private ExecutionStep createFailAlwaysExecutionStep(List<OnFailureExecutionStep> onFailureExecutionSteps) {
        return new ExecutionStep("fail1", (Doc doc) -> ProcessResult.failure("test failure"), onFailureExecutionSteps);
    }
}
