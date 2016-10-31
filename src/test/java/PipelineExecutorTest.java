import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.Processor;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import logback.TestingAppenderFactory;
import logback.WaitForAppender;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertNotNull;

public class PipelineExecutorTest {
    public static final long THRESHOLD_TIME = 1000;
    private PipelineExecutor pipelineExecutor;
    private TestingAppenderFactory testingAppenderFactory;


    @Before
    public void init() {
        pipelineExecutor = new PipelineExecutor(THRESHOLD_TIME);
        testingAppenderFactory = new TestingAppenderFactory();
    }

    @Test
    public void testPipelineLongProcessingExecution() throws InterruptedException{
        String id = "abc";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void process(Doc log) {
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

        String searchString = String.format("processing %s takes too long, more than threshold=%s",doc, THRESHOLD_TIME);

        WaitForAppender appender = testingAppenderFactory.createWaitForAppender(PipelineExecutor.class, searchString);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertThat(appender.foundSearchString()).isTrue();

        testingAppenderFactory.removeWaitForAppender(PipelineExecutor.class, appender);
    }

    @Test
    public void testPipelineExecution() throws PipelineExecutionException {
        String id = "abc";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void process(Doc doc) {
                doc.addFieldValue("newField", "Hello");
            }

            @Override
            public String getName() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processors);
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
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void process(Doc doc) {
                throw new RuntimeException("test failure");
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

        String searchString = "pipeline failed";

        WaitForAppender appender = testingAppenderFactory.createWaitForAppender(PipelineExecutor.class, searchString);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertThat(appender.foundSearchString()).isTrue();

        testingAppenderFactory.removeWaitForAppender(PipelineExecutor.class, appender);
    }
}
