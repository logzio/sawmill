package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class AddAndRemoveTagProcessorTest {
    @Test
    public void testAddAndRemoveSingleTag() {
        AddTagProcessor addTagProcessor = new AddTagProcessor(Arrays.asList("test_tag"));
        RemoveTagProcessor removeTagProcessor = new RemoveTagProcessor(Arrays.asList("test_tag"));
        Doc doc = createDoc("field1", "value");

        addTagProcessor.process(doc);

        assertThat((List)doc.getField("tags")).contains("test_tag");

        removeTagProcessor.process(doc);

        assertThat(((List)doc.getField("tags")).contains("test_tag")).isFalse();
    }

    @Test
    public void testAddAndRemoveSeveralTags() {
        List<String> tags = Arrays.asList("tag1", "tag2", "tag3");
        AddTagProcessor addTagProcessor = new AddTagProcessor(tags);
        RemoveTagProcessor removeTagProcessor = new RemoveTagProcessor(tags);
        Doc doc = createDoc("field1", "value");

        addTagProcessor.process(doc);

        assertThat((List)doc.getField("tags")).containsAll(tags);

        removeTagProcessor.process(doc);

        assertThat(((List)doc.getField("tags")).containsAll(tags)).isFalse();
    }
}
