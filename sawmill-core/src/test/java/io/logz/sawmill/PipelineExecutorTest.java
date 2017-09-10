package io.logz.sawmill;

import io.logz.sawmill.conditions.AndCondition;
import io.logz.sawmill.conditions.FieldExistsCondition;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.processors.GrokProcessor;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class PipelineExecutorTest {
    private static final long WARNING_THRESHOLD_TIME_MS = 500;
    private static final long EXPIRED_THRESHOLD_TIME_MS = 2000;

    private PipelineExecutor pipelineExecutor;
    private List<Doc> overtimeProcessingDocs;
    private PipelineExecutionMetricsMBean pipelineExecutorMetrics;

    @Before
    public void init() {
        overtimeProcessingDocs = new ArrayList<>();
        pipelineExecutorMetrics = new PipelineExecutionMetricsMBean();
        PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(WARNING_THRESHOLD_TIME_MS, EXPIRED_THRESHOLD_TIME_MS, pipelineExecutorMetrics,
                watchedPipeline -> overtimeProcessingDocs.add(watchedPipeline.getDoc()));
        pipelineExecutor = new PipelineExecutor(watchdog, pipelineExecutorMetrics);
    }

    @Test
    public void testWarnLongProcessingExecution() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createSleepExecutionStep(WARNING_THRESHOLD_TIME_MS + 300)
        );
        Doc doc = createDoc("id", "testWarnLongProcessingExecution", "message", "hola",
                "type", "test");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isOvertime()).isTrue();
        assertThat(executionResult.getOvertimeTook().get()).isGreaterThan(WARNING_THRESHOLD_TIME_MS);

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsProcessingExpired()).isEqualTo(0);
    }

    @Test
    public void testKillLongProcessingExecution() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createSleepExecutionStep(EXPIRED_THRESHOLD_TIME_MS + 300)
        );
        Doc doc = createDoc("id", "testKillLongProcessingExecution", "message", "hola",
                "type", "test");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isExpired()).isTrue();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsProcessingExpired()).isEqualTo(1);
    }

    @Test
    public void testKillLongGrokExecution() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createLongGrokExecutionStep()
        );
        Doc doc = createDoc("id", "testKillLongGrokExecution",
                "message", RandomStringUtils.random(10000),
                "type", "test");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isExpired()).isTrue();
        assertThat(Thread.currentThread().isInterrupted()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.getTotalDocsOvertimeProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsProcessingExpired()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecution() {
        Pipeline pipeline = createPipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createAddFieldExecutionStep("newField2", "value2")
        );
        Doc doc = createDoc("id", "testPipelineExecution", "message", "hola");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(executionResult.isOvertime()).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testOnSuccessPipelineExecution() {
        Pipeline pipeline = createPipeline(
                createExecutionStepWithOnSuccessSteps(
                        createAddFieldProcessor("newField1", "value1"),
                        createAddFieldExecutionStep("newField2", "value2"),
                        createAddFieldExecutionStep("newField3", "value3")));

        Doc doc = createDoc("id", "testPipelineExecution", "message", "hola");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(doc.getSource().get("newField3")).isEqualTo("value3");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(executionResult.isOvertime()).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testOnSuccessNotWorkingOnFailPipelineExecution() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(null,
                        Arrays.asList(createAddFieldExecutionStep("newField2", "value2"), createAddFieldExecutionStep("newField3", "value3"))
                ));
        Doc doc = createDoc("id", "testOnFailureExecutionSteps", "message", "hola");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isNull();
        assertThat(doc.getSource().get("newField3")).isNull();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(executionResult.isOvertime()).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }


    @Test
    public void testOnFailureExecutionSteps() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(
                        createAddFieldExecutionStep("newField2", "value2"),
                        createAddFieldExecutionStep("newField3", "value3")
                ));
        Doc doc = createDoc("id", "testOnFailureExecutionSteps", "message", "hola");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isTrue();
        assertThat(executionResult.isOvertime()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(doc.getSource().get("newField3")).isEqualTo("value3");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testFailOnFailureExecutionStep() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(
                        createAddFieldExecutionStep("newField2", "value2")),
                        createFailAlwaysExecutionStep()
                );

        Doc doc = createDoc("id", "testFailOnFailureExecutionStep", "message", "hola");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(doc.getSource().get("newField2")).isEqualTo("value2");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(executionResult.isOvertime()).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testFailure() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep()
        );
        Doc doc = createDoc("id", "testFailure", "message", "hola",
                "type", "test");

        ExecutionResult result = pipelineExecutor.execute(pipeline, doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(result.isOvertime()).isFalse();
        assertThat(result.getError().get().getException().isPresent()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testDrop() {
        Pipeline pipeline = createPipeline(createAddFieldExecutionStep("newField1", "value1"),
                createDropExecutionStep()
        );
        Doc doc = createDoc("id", "testDrop", "message", "hola",
                "type", "test");

        ExecutionResult result = pipelineExecutor.execute(pipeline, doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(result.isDropped()).isTrue();
        assertThat(result.isOvertime()).isFalse();

        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsDropped()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testFailureWithException() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createAddFieldExecutionStep("newField1", "value1"),
                createFailAlwaysExecutionStep(new ProcessorExecutionException("failProcessor", new RuntimeException("fail message")))
        );
        Doc doc = createDoc("id", "testFailureWithException", "message", "hola",
                "type", "test");

        ExecutionResult result = pipelineExecutor.execute(pipeline, doc);
        assertThat(result.isSucceeded()).isFalse();
        assertThat(result.isOvertime()).isFalse();
        assertThat(result.getError().get().getException().isPresent()).isTrue();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testStopOnFailure() {
        Pipeline pipeline = createStopOnFailurePipeline(
                createFailAlwaysExecutionStep(),
                createAddFieldExecutionStep("newField1", "value1")
        );
        Doc doc = createDoc("id", "testStopOnFailure", "message", "hola",
                "type", "test");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isFalse();
        assertThat(executionResult.isOvertime()).isFalse();

        assertThat(doc.getSource().get("newField1")).isNull();
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getProcessingFailedCount("fail1")).isEqualTo(1);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(0);
    }

    @Test
    public void testIgnoreFailure() {
        Pipeline pipeline = createPipeline(
                createFailAlwaysExecutionStep(),
                createAddFieldExecutionStep("newField1", "value1")
        );
        Doc doc = createDoc("id", "testStopOnFailure", "message", "hola",
                "type", "test");

        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc);
        assertThat(executionResult.isSucceeded()).isTrue();
        assertThat(executionResult.isOvertime()).isFalse();

        assertThat(doc.getSource().get("newField1")).isEqualTo("value1");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.getTotalDocsFailedProcessing()).isEqualTo(0);
        assertThat(pipelineExecutorMetrics.getTotalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testUnexpectedFailure() {
        Pipeline pipeline = createPipeline(
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
        ExecutionResult executionResult = pipelineExecutor.execute(pipeline, doc1);
        assertThat(executionResult.isSucceeded()).isTrue();
        assertThat(executionResult.isOvertime()).isFalse();
        assertThat(doc1.getSource().get("newField1")).isEqualTo("value1");
        String value1 = doc1.getField(fieldToAdd);
        assertThat(value1).isEqualTo(valueOnTrue);

        Doc doc2 = createDoc(fieldExists1, "value3");
        ExecutionResult executionResult2 = pipelineExecutor.execute(pipeline, doc2);
        assertThat(executionResult2.isSucceeded()).isTrue();
        assertThat(executionResult2.isOvertime()).isFalse();
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

    private Pipeline createStopOnFailurePipeline(ExecutionStep... steps) {
        return createPipeline(true, steps);
    }

    private Pipeline createPipeline(boolean stopOnFailure, ExecutionStep... steps) {
        String id = "abc";
        return new Pipeline(id, Arrays.asList(steps), stopOnFailure);
    }

    private ProcessorExecutionStep createSleepExecutionStep(long millis) {
        return new ProcessorExecutionStep("sleep1", (Doc doc) -> {

            Thread.sleep(millis);

            return ProcessResult.success();
        });
    }

    private ExecutionStep createLongGrokExecutionStep() {
        return new ProcessorExecutionStep("slow greedy grok",
                createProcessor(GrokProcessor.class,
                        "field", "message",
                        "patterns", Arrays.asList(".{10000,}.{100000}")));
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

    private ProcessorExecutionStep createExecutionStepWithOnSuccessSteps(Processor processor, ExecutionStep... onSuccessSteps) {
        return new ProcessorExecutionStep("success1", processor, null, Arrays.asList(onSuccessSteps));
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

    private ProcessorExecutionStep createFailAlwaysExecutionStep(ExecutionStep... onFailureExecutionSteps) {
        return createFailAlwaysExecutionStep(Arrays.asList(onFailureExecutionSteps), null);
    }

    private ProcessorExecutionStep createFailAlwaysExecutionStep(List<ExecutionStep> onFailureExecutionSteps, List<ExecutionStep> onSuccessExecutionSteps) {
        return new ProcessorExecutionStep("fail1", createFailAlwaysProcessor(), onFailureExecutionSteps, onSuccessExecutionSteps);
    }

    private ExecutionStep createDropExecutionStep() {
        return new ProcessorExecutionStep("drop1", createDropProcessor());
    }

    private Processor createDropProcessor() {
        return (Doc doc) -> ProcessResult.drop();
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
