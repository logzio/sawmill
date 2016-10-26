import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.Process;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.junit.Before;
import org.junit.Test;
import uk.org.lidalia.slf4jtest.TestLogger;
import uk.org.lidalia.slf4jtest.TestLoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;
import static uk.org.lidalia.slf4jtest.LoggingEvent.error;
import static uk.org.lidalia.slf4jtest.LoggingEvent.warn;

public class PipelineExecutorTest {
    public static final long THRESHOLD_TIME = 1000;
    private PipelineExecutor pipelineExecutor;
    private TestLogger logger;

    @Before
    public void init() {
        pipelineExecutor = new PipelineExecutor(THRESHOLD_TIME);
        logger = TestLoggerFactory.getTestLogger(PipelineExecutor.class);
    }

    @Test
    public void testPipelineLongProcessingExecution() throws InterruptedException{
        String id = "abc";
        String description = "test";
        ArrayList<Process> processors = new ArrayList<>();
        processors.add(new Process() {
            @Override
            public void execute(Doc log) {
                try {
                    Thread.sleep(1100);
                } catch (InterruptedException e) {

                }
            }

            @Override
            public String getName() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processors);
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Doc doc = new Doc(source);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertThat(logger.getAllLoggingEvents().contains(warn("processing {} takes too long, more than threshold={}", doc, THRESHOLD_TIME))).isTrue();
    }

    @Test
    public void testPipelineExecution() throws PipelineExecutionException {
        String id = "abc";
        String description = "test";
        ArrayList<Process> processes = new ArrayList<>();
        processes.add(new Process() {
            @Override
            public void execute(Doc doc) {
                doc.addFieldValue("newField", "Hello");
            }

            @Override
            public String getName() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processes);
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        Doc doc = new Doc(source);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertNotNull(doc.getSource().get("newField"));
    }

    @Test
    public void testPipelineExecutionFailure() throws PipelineExecutionException {
        String id = "abc";
        String description = "test";
        ArrayList<Process> processes = new ArrayList<>();
        processes.add(new Process() {
            @Override
            public void execute(Doc doc) {
                throw new RuntimeException("test failure");
            }

            @Override
            public String getName() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processes);
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Doc doc = new Doc(source);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertThat(logger.getAllLoggingEvents().contains(error("pipeline failed"))).isTrue();
    }
}
