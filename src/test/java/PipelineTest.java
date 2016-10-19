import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Log;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.Processor;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.processors.TestProcessor;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.assertNotNull;

public class PipelineTest {

    @Test
    public void testConstructWithOneProcessor() {
        String id = "abc";
        String description = "test";
        Double version = 1.0;
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void execute(Log log) {

            }

            @Override
            public String getType() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processors);

        assertThat(pipeline.getProcessors().size()).isEqualTo(1);
        assertThat(pipeline.getProcessors().get(0).getType()).isEqualTo("test");
    }

    @Test
    public void testConstructWithInvalidArguments() {
        assertThatThrownBy(() -> new Pipeline("", "", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Pipeline("abc", "", null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new Pipeline("abc", "", new ArrayList<>())).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testExecute() throws PipelineExecutionException {
        String id = "abc";
        String description = "test";
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void execute(Log log) {
                log.addFieldValue("newField", "Hello");
            }

            @Override
            public String getType() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processors);
        Map<String,Object> source = new HashMap<>();
        source.put("message", "hola");
        Log log = new Log(source);
        pipeline.execute(log);

        assertNotNull(log.getSource().get("newField"));
    }

    @Test
    public void testExecuteFailure() throws PipelineExecutionException {
        String id = "abc";
        String description = "test";
        Double version = 1.0;
        ArrayList<Processor> processors = new ArrayList<>();
        processors.add(new Processor() {
            @Override
            public void execute(Log log) {
                throw new RuntimeException("test failure");
            }

            @Override
            public String getType() {
                return  "test";
            }
        });
        Pipeline pipeline = new Pipeline(id, description, processors);
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Log log = new Log(source);

        assertThatThrownBy(() -> pipeline.execute(log)).isInstanceOf(PipelineExecutionException.class);
    }

    @Test
    public void testFactoryCreate() {
        String configJson = "{" +
                    "\"id\": \"abc\"," +
                    "\"description\": \"this is pipeline configuration\"," +
                    "\"processors\": [{" +
                        "\"name\": \"test\"," +
                        "\"config\": {" +
                            "\"value\": \"message\"" +
                        "}" +
                    "}]" +
                "}";
        Pipeline.Factory factory = new Pipeline.Factory();
        Pipeline pipeline = factory.create(JsonUtils.fromJsonString(Pipeline.Configuration.class, configJson));

        assertThat(pipeline.getId()).isEqualTo("abc");
        assertThat(pipeline.getDescription()).isEqualTo("this is pipeline configuration");
        assertThat(pipeline.getProcessors().size()).isEqualTo(1);
        assertThat(pipeline.getProcessors().get(0).getType()).isEqualTo("test");
        assertThat(((TestProcessor)pipeline.getProcessors().get(0)).getValue()).isEqualTo("message");
    }
}
