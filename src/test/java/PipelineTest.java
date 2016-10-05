import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Log;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.Processor;
import org.json.simple.JSONObject;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.fail;
import static org.junit.Assert.assertNotNull;

public class PipelineTest {

    @Test
    public void testConstructWithOneProcessor() {
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
        Pipeline pipeline = new Pipeline(processors);

        assertThat(pipeline.getProcessors().size()).isEqualTo(1);
        assertThat(pipeline.getProcessors().get(0).getType()).isEqualTo("test");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructWithNull() {
        new Pipeline(null);
        fail("Constructor should have thrown an exception when no processors");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructWithEmptyList() {
        new Pipeline(new ArrayList<>());
        fail("Constructor should have thrown an exception when no processors");
    }

    @Test
    public void testExecute() {
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
        Pipeline pipeline = new Pipeline(processors);
        Map<String,Object> source = ImmutableMap.of("message", "hola",
                "type", "test");
        Log log = new Log(new JSONObject(source));
        pipeline.execute(log);

        assertNotNull(log.getSource().get("newField"));
    }
}
