package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class AddAndRemoveFieldProcessorTest {

    @Test
    public void testRemoveField() {
        String path = "message.hola.hello";
        AddFieldProcessor addFieldProcessor = new AddFieldProcessor(path, "shalom");
        RemoveFieldProcessor removeFieldIgnoreMissingProcessor = new RemoveFieldProcessor(path, true);
        RemoveFieldProcessor removeFieldDontIgnoreMissingProcessor = new RemoveFieldProcessor(path, false);

        Doc doc = createDoc("field", "value");

        removeFieldIgnoreMissingProcessor.process(doc);

        addFieldProcessor.process(doc);

        assertThat((String) doc.getField(path)).isEqualTo("shalom");

        removeFieldDontIgnoreMissingProcessor.process(doc);

        assertThatThrownBy(() ->  doc.getField(path)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> removeFieldDontIgnoreMissingProcessor.process(doc)).isInstanceOf(IllegalStateException.class);
    }
}
