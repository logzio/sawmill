package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AddAndRemoveFieldProcessorTest {

    @Test
    public void testAddAndRemoveField() {
        String path = "message.hola.hello";
        AddFieldProcessor addFieldProcessor = new AddFieldProcessor(path, "shalom");
        RemoveFieldProcessor removeFieldProcessor = new RemoveFieldProcessor(path);

        Doc doc = createDoc("field", "value");

        assertThat(addFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String) doc.getField(path)).isEqualTo("shalom");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThatThrownBy(() ->  doc.getField(path)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRemoveNonExistsField() {
        String path = "message.hola.hello";
        RemoveFieldProcessor removeFieldProcessor = new RemoveFieldProcessor(path);

        Doc doc = createDoc("field", "value");

        assertThat(removeFieldProcessor.process(doc).isSucceeded()).isFalse();
    }
}
