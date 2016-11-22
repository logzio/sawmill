package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AddAndRemoveFieldProcessorTest {

    @Test
    public void testAddAndRemoveFieldIgnoreFailure() {
        String path = "message.hola.hello";
        AddFieldProcessor addFieldProcessor = new AddFieldProcessor(path, "shalom");
        RemoveFieldProcessor removeFieldProcessor = new RemoveFieldProcessor(path, Collections.EMPTY_LIST, true);

        Doc doc = createDoc("field", "value");

        // Tests ignore failure
        removeFieldProcessor.process(doc);

        addFieldProcessor.process(doc);

        assertThat((String) doc.getField(path)).isEqualTo("shalom");

        removeFieldProcessor.process(doc);

        assertThatThrownBy(() ->  doc.getField(path)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRemoveNonExistsFieldAndAddInCaseOfFailure() {
        String path = "message.hola.hello";
        AddFieldProcessor addFieldProcessor = new AddFieldProcessor(path, "shalom");
        RemoveFieldProcessor removeFieldProcessor = new RemoveFieldProcessor(path, Arrays.asList(addFieldProcessor), false);

        Doc doc = createDoc("field", "value");

        removeFieldProcessor.process(doc);

        assertThat((String) doc.getField(path)).isEqualTo("shalom");

        removeFieldProcessor.process(doc);

        assertThatThrownBy(() ->  doc.getField(path)).isInstanceOf(IllegalStateException.class);
    }
}
