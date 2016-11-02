import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.Processor;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class PipelineExecutorTest {
    public static final long THRESHOLD_TIME_MS = 1000;

    private PipelineExecutor pipelineExecutor;
    private List<Doc> overtimeProcessingDocs = new ArrayList<>();

    @Before
    public void init() {
        pipelineExecutor = new PipelineExecutor(THRESHOLD_TIME_MS, context -> {
            overtimeProcessingDocs.add(context.getDoc());
        });
    }

    @Test
    public void testPipelineLongProcessingExecution() throws InterruptedException{
        String id = "abc";
        String name = "test";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(createSleepProcessor(1100));
        Pipeline pipeline = new Pipeline(id, name, description, processors);
        Doc doc = createDoc("message", "hola",
                "type", "test");

        pipelineExecutor.executePipeline(pipeline, doc);

        assertThat(overtimeProcessingDocs.contains(doc)).isTrue();
    }

    private Doc createDoc(Object... objects) {
        LinkedHashMap map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put(objects[i], objects[++i]);
            }
        }
        return new Doc(map);
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
                return  "test";
            }
        };
    }

    @Test
    public void testPipelineExecution() throws PipelineExecutionException {
        String id = "abc";
        String name = "test";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(createAddFieldProcessor("newField", "Hello"));
        Pipeline pipeline = new Pipeline(id, name, description, processors);
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        Doc doc = new Doc(source);

        pipelineExecutor.executePipeline(pipeline, doc);

        assertNotNull(doc.getSource().get("newField"));
    }

    private Processor createAddFieldProcessor(String k, String v) {
        return new Processor() {
            @Override
            public void process(Doc doc) {
                doc.addFieldValue(k, v);
            }

            @Override
            public String getName() {
                return  "test";
            }
        };
    }

    @Test
    public void testPipelineExecutionFailure() throws PipelineExecutionException {
        String id = "abc";
        String name = "test";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(createFailAlwaysProcessor());
        Pipeline pipeline = new Pipeline(id, name, description, processors);
        Doc doc = createDoc("message", "hola",
                "type", "test");

        assertThatThrownBy(() -> pipelineExecutor.executePipeline(pipeline, doc)).isInstanceOf(PipelineExecutionException.class);
    }

    private Processor createFailAlwaysProcessor() {
        return new Processor() {
            @Override
            public void process(Doc doc) {
                throw new RuntimeException("test failure");
            }

            @Override
            public String getName() {
                return  "test";
            }
        };
    }
}
