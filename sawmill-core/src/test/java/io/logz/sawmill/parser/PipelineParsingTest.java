package io.logz.sawmill.parser;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import org.assertj.core.util.Maps;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class PipelineParsingTest {
    @Test
    public void testParsingPipelineWithSingleRename() throws IOException {
        String json = getFileResourceAsString("single-rename.json");
        Pipeline pipeline = new Pipeline.Factory().create(json);
        PipelineExecutor pipelineExecutor = new PipelineExecutor();

        Doc doc = new Doc(Maps.newHashMap("FieldFrom1", "MyValue1"));
        pipelineExecutor.execute(pipeline, doc);
        assertThat((String) doc.getField("FieldTo1")).isEqualTo("MyValue1");
    }

    @Test
    public void testParsingPipelineWithMultipleRenamesInOneProcessorFromJson() throws IOException {
        String json = getFileResourceAsString("multiple-renames-in-one-processor.json");
        validateParsingPipelineWithMultipleRenamesInOneProcessor(json);
    }

    @Test
    public void testParsingPipelineWithMultipleRenamesInOneProcessoFromHocon() throws IOException {
        String json = getFileResourceAsString("multiple-renames-in-one-processor.hocon");
        validateParsingPipelineWithMultipleRenamesInOneProcessor(json);
    }

    private void validateParsingPipelineWithMultipleRenamesInOneProcessor(String pipelineText) {
        Pipeline pipeline = new Pipeline.Factory().create(pipelineText);
        PipelineExecutor pipelineExecutor = new PipelineExecutor();

        Map<String, Object> map = new HashMap<>(
                ImmutableMap.of(
                        "FieldFrom1", "MyValue1",
                        "FieldFrom2", "MyValue2"
                )
        );
        Doc doc = new Doc(map);

        pipelineExecutor.execute(pipeline, doc);

        assertThat((String) doc.getField("FieldTo1")).isEqualTo("MyValue1");
        assertThat((String) doc.getField("FieldTo2")).isEqualTo("MyValue2");

    }

    private String getFileResourceAsString(final String resourceName) throws IOException {
        File file = new File(
                this.getClass().getResource(
                        "/" + this.getClass().getPackage().getName() + "/" + resourceName
                ).getFile());
        return String.join("\n", Files.readLines(file, Charset.defaultCharset()));
    }
}
