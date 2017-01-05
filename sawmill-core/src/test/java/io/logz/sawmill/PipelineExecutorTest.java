package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.FieldExistsCondition;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
                watchedPipeline -> overtimeProcessingDocs.add(watchedPipeline.getDoc()));
        pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
    }

    @Test
    public void testLongProcessingExecution() throws InterruptedException {
        Pipeline pipeline = createPipeline(false,
                createAddFieldExecutionStep("newField1", "value1"),
                createSleepExecutionStep(1100)
        );
        Doc doc = createDoc("id", "testLongProcessingExecution", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecution() {
        Pipeline pipeline = createPipeline(false,
                createAddFieldExecutionStep("newField1", "value1"),
                createAddFieldExecutionStep("newField2", "value2")
        );
        Doc doc = createDoc("id", "testPipelineExecution", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testOnFailureExecutionSteps() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(
                        createOnFailureExecutionStep(createAddFieldProcessor("newField2", "value2")),
                        createOnFailureExecutionStep(createAddFieldProcessor("newField3", "value3"))
                ));
        Doc doc = createDoc("id", "testOnFailureExecutionSteps", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(doc.getSource().get("newField3")).isEqualTo("value3");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testFailOnFailureExecutionStep() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(
                        createOnFailureExecutionStep(createAddFieldProcessor("newField2", "value2")),
                        createOnFailureExecutionStep(createFailAlwaysProcessor())
                ));

        Doc doc = createDoc("id", "testFailOnFailureExecutionStep", "message", "hola");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testFailure() {
        Pipeline pipeline = createPipeline(false,
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep()
        );
        Doc doc = createDoc("id", "testFailure", "message", "hola",
                "type", "test");

        ExecutionResult result = pipelineExecutor.execute(pipeline, doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(result.getError().get().getException().isPresent()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testFailureWithException() {
        Pipeline pipeline = createPipeline(false,
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(new ProcessorExecutionException("failProcessor", new RuntimeException("fail message")))
        );
        Doc doc = createDoc("id", "testFailureWithException", "message", "hola",
                "type", "test");

        ExecutionResult result = pipelineExecutor.execute(pipeline, doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(result.getError().get().getException().isPresent()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testIgnoreFailure() {
        Pipeline pipeline = createPipeline(true,
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep()
        );
        Doc doc = createDoc("id", "testIgnoreFailure", "message", "hola",
                "type", "test");

        assertThat(pipelineExecutor.execute(pipeline, doc).isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getProcessingFailedCount("fail1")).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testUnexpectedFailure() {
        Pipeline pipeline = createPipeline(false,
                createAddFieldExecutionStep("newField1", "value1"),
                createUnexpectedFailAlwaysExecutionStep()
        );
        Doc doc = createDoc("id", "testUnexpectedFailure", "message", "hola",
                "type", "test");

        assertThatThrownBy(() -> pipelineExecutor.execute(pipeline, doc)).isInstanceOf(PipelineExecutionException.class);

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
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

        Pipeline pipeline = createPipeline(createConditionalExecutionStep(
                createExistsCondition(fieldExists1, fieldExists2),
                createExecutionSteps(
                        createAddFieldExecutionStep("newField1", "value1"),
                        createAddFieldExecutionStep(fieldToAdd, valueOnTrue)
                ),
                createExecutionSteps(
                        createAddFieldExecutionStep("newField1", "value1"),
                        createAddFieldExecutionStep(fieldToAdd, valueOnFalse))));

        Doc doc1 = createDoc(fieldExists1, "value1", fieldExists2, "value2");
        assertThat(pipelineExecutor.execute(pipeline, doc1).isSucceeded()).isTrue();
        assertThat(doc1.getSource().get("newField1")).isEqualTo("value1");
        String value1 = doc1.getField(fieldToAdd);
        assertThat(value1).isEqualTo(valueOnTrue);

        Doc doc2 = createDoc(fieldExists1, "value3");
        assertThat(pipelineExecutor.execute(pipeline, doc2).isSucceeded()).isTrue();
        assertThat(doc2.getSource().get("newField1")).isEqualTo("value1");
        String value2 = doc2.getField(fieldToAdd);
        assertThat(value2).isEqualTo(valueOnFalse);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(2);
    }

    private List<ExecutionStep> createExecutionSteps(ExecutionStep... steps) {
        return Arrays.asList(steps);
    }

    private Pipeline createPipeline(ExecutionStep... steps) {
        return createPipeline(false, steps);
    }

    private Pipeline createPipeline(boolean ignoreFailure, ExecutionStep... steps) {
        String id = "abc";
        String name = "test";
        String description = "test";
        return new Pipeline(id, name, description, Arrays.asList(steps), ignoreFailure);
    }

    private OnFailureExecutionStep createOnFailureExecutionStep(Processor processor) {
        String name = "on failure processor";
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

    private ProcessorExecutionStep createFailAlwaysExecutionStep() {
        return new ProcessorExecutionStep("fail1", createFailAlwaysProcessor(), null);
    }

    private ProcessorExecutionStep createFailAlwaysExecutionStep(ProcessorExecutionException e) {
        return new ProcessorExecutionStep("fail1", createFailAlwaysProcessor(e), null);
    }

    private ProcessorExecutionStep createFailAlwaysExecutionStep(OnFailureExecutionStep... onFailureExecutionSteps) {
        return new ProcessorExecutionStep("fail1", createFailAlwaysProcessor(), Arrays.asList(onFailureExecutionSteps));
    }

    private Processor createFailAlwaysProcessor() {
        return (Doc doc) -> ProcessResult.failure("test failure");
    }

    private Processor createFailAlwaysProcessor(ProcessorExecutionException e) {
        return (Doc doc) -> ProcessResult.failure("test failure", e);
    }

    private ConditionalExecutionStep createConditionalExecutionStep(Condition condition, List<ExecutionStep> onTrue, List<ExecutionStep> onFalse) {
        return new ConditionalExecutionStep(condition, onTrue, onFalse);
    }

    private Condition createExistsCondition(String field1, String field2) {
        return new AndCondition(Arrays.asList(new FieldExistsCondition(field1), new FieldExistsCondition(field2)));
    }
}
