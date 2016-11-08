package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class RenameFieldProcessorTest {
    @Test
    public void testRenameField() {
        String fromField = "from";
        String toField = "to";
        Doc doc = createDoc(fromField, "value");

        RenameFieldProcessor renameFieldProcessor = new RenameFieldProcessor(fromField, toField);

        renameFieldProcessor.process(doc);

        assertThat((String)doc.getField(toField)).isEqualTo("value");
    }
}
