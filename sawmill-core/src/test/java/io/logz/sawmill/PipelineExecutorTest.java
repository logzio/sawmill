package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.FieldExistsCondition;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
                context -> overtimeProcessingDocs.add(context.getDoc()));
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

    @Test
    public void testConditionalExecutionStep() {
        String fieldExists1 = "fieldExists1";
        String fieldExists2 = "fieldExists2";
        String fieldToAdd = "fieldToAdd";
        String valueOnTrue = "valueOnTrue";
        String valueOnFalse = "valueOnFalse";
        Pipeline pipeline = createPipeline(false, createConditionalExecutionStep(
                createExistsCondition(fieldExists1, fieldExists2),
                createAddFieldExecutionStep(fieldToAdd, valueOnTrue),
                createAddFieldExecutionStep(fieldToAdd, valueOnFalse)));

        Doc doc1 = createDoc(fieldExists1, "value1", fieldExists2, "value2");
        assertThat(pipelineExecutor.execute(pipeline, doc1).isSucceeded()).isTrue();
        String value1 = doc1.getField(fieldToAdd);
        assertThat(value1).isEqualTo(valueOnTrue);

        Doc doc2 = createDoc(fieldExists1, "value3");
        assertThat(pipelineExecutor.execute(pipeline, doc2).isSucceeded()).isTrue();
        String value2 = doc2.getField(fieldToAdd);
        assertThat(value2).isEqualTo(valueOnFalse);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(2);
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

    private ProcessorExecutionStep createSleepExecutionStep(long millis) {
        return new ProcessorExecutionStep("sleep1", (Doc doc) -> {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {

                }
                return ProcessResult.success();
            });
    }

    private ProcessorExecutionStep createAddFieldExecutionStep(String k, String v) {
        return new ProcessorExecutionStep("add1", createAddFieldProcessor(k, v));
    }

    private Processor createAddFieldProcessor(String k, String v) {
        return (Doc doc) -> {
                doc.addField(k, v);
                return ProcessResult.success();
            };
    }

    private ProcessorExecutionStep createUnexpectedFailAlwaysExecutionStep() {
        return new ProcessorExecutionStep("failHard1", (Doc doc) -> {
                throw new RuntimeException("test failure");
            });
    }

    private ProcessorExecutionStep createFailAlwaysExecutionStep(List<OnFailureExecutionStep> onFailureExecutionSteps) {
        return new ProcessorExecutionStep("fail1", (Doc doc) -> ProcessResult.failure("test failure"), onFailureExecutionSteps);
    }

    private ConditionalExecutionStep createConditionalExecutionStep(Condition condition, ExecutionStep onTrue, ExecutionStep onFalse) {
        return new ConditionalExecutionStep(condition, Collections.singletonList(onTrue), Collections.singletonList(onFalse));
    }

    private Condition createExistsCondition(String field1, String field2) {
        return new AndCondition(Arrays.asList(new FieldExistsCondition(field1), new FieldExistsCondition(field2)));
    }
}
