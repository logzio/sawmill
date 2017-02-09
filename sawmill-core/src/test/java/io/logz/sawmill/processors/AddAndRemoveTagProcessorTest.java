package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class AddAndRemoveTagProcessorTest {
    @Test
    public void testAddAndRemoveSingleTagWhileTagsFieldIsNotList() {
        List<String> tags = Arrays.asList("test_tag");

        Map<String,Object> config = createConfig("tags", tags);

        AddTagProcessor addTagProcessor = createProcessor(AddTagProcessor.class, config);
        RemoveTagProcessor removeTagProcessor = createProcessor(RemoveTagProcessor.class, config);
        Doc doc = createDoc("tags", "value");

        // Tests no exception thrown in case of none tags
        assertThat(removeTagProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(addTagProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(((List)doc.getField("tags")).contains("value")).isTrue();
        assertThat(((List)doc.getField("tags")).contains("test_tag")).isTrue();

        assertThat(removeTagProcessor.process(doc).isSucceeded()).isTrue();

        assertThat(((List)doc.getField("tags")).contains("test_tag")).isFalse();
    }

    @Test
    public void testAddAndRemoveSeveralTagsWhileTagsFieldMissing() {
        List<String> tags = Arrays.asList("tag1", "tag2", "tag3");

        Map<String,Object> config = createConfig("tags", tags);

        AddTagProcessor addTagProcessor = createProcessor(AddTagProcessor.class, config);
        RemoveTagProcessor removeTagProcessor = createProcessor(RemoveTagProcessor.class, config);
        Doc doc = createDoc("field1", "value");

        assertThat(addTagProcessor.process(doc).isSucceeded()).isTrue();

        for (String tag : tags) {
            assertThat(((List) doc.getField("tags")).contains(tag)).isTrue();
        }

        assertThat(removeTagProcessor.process(doc).isSucceeded()).isTrue();

        for (String tag : tags) {
            assertThat(((List) doc.getField("tags")).contains(tag)).isFalse();
        }
        assertThat((String) doc.getField("field1")).isEqualTo("value");
    }
}
