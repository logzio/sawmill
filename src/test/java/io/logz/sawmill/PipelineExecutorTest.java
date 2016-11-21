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
    public PipelineExecutionMetricsTracker pipelineExecutorMetrics;
    public SawmillConfig sawmillConfig;

    @Before
    public void init() {
        overtimeProcessingDocs = new ArrayList<>();
        sawmillConfig = new SawmillConfig();
        pipelineExecutorMetrics = sawmillConfig.getMetricsTracker();
        PipelineExecutionTimeWatchdog watchdog = new PipelineExecutionTimeWatchdog(THRESHOLD_TIME_MS, pipelineExecutorMetrics,
                context -> overtimeProcessingDocs.add(context.getDoc()));
        sawmillConfig.setWatchdog(watchdog);
        pipelineExecutor = new PipelineExecutor(sawmillConfig);
    }

    @Test
    public void testPipelineLongProcessingExecution() throws InterruptedException{
        Pipeline pipeline = createPipeline(createSleepProcessor(1100));
        Doc doc = createDoc("id", "long", "message", "hola",
                "type", "test");

        pipelineExecutor.execute(pipeline, doc);

        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
        assertThat(pipelineExecutorMetrics.totalDocsOvertimeProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecution() throws PipelineExecutionException {
        Pipeline pipeline = createPipeline(createAddFieldProcessor("newField", "Hello"));
        Doc doc = createDoc("id", "add", "message", "hola");

        pipelineExecutor.execute(pipeline, doc);

        assertNotNull(doc.getSource().get("newField"));
        assertThat(doc.getSource().get("newField")).isEqualTo("Hello");
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.totalDocsSucceededProcessing()).isEqualTo(1);
    }

    @Test
    public void testPipelineExecutionFailure() throws PipelineExecutionException {
        Pipeline pipeline = createPipeline(createFailAlwaysProcessor());
        Doc doc = createDoc("id", "fail", "message", "hola",
                "type", "test");

        assertThatThrownBy(() -> pipelineExecutor.execute(pipeline, doc)).isInstanceOf(PipelineExecutionException.class);
        assertThat(overtimeProcessingDocs.contains(doc)).isFalse();
        assertThat(pipelineExecutorMetrics.totalDocsFailedProcessing()).isEqualTo(1);
    }

    private Pipeline createPipeline(Processor... processors) {
        String id = "abc";
        String name = "test";
        String description = "test";
        return new Pipeline(id, name, description, Arrays.asList(processors));
    }

    private Processor createSleepProcessor(long millis) {
        return new Processor() {
            @Override
            public void process(Doc log) {
                try {
                    Thread.sleep(millis);
                } catch (InterruptedException e) {

                }
            }

            @Override
            public String getName() {
                return  "sleep";
            }
        };
    }

    private Processor createAddFieldProcessor(String k, String v) {
        return new Processor() {
            @Override
            public void process(Doc doc) {
                doc.addField(k, v);
            }

            @Override
            public String getName() {
                return  "addField";
            }
        };
    }

    private Processor createFailAlwaysProcessor() {
        return new Processor() {
            @Override
            public void process(Doc doc) {
                throw new RuntimeException("test failure");
            }

            @Override
            public String getName() {
                return  "fail";
            }
        };
    }
}
