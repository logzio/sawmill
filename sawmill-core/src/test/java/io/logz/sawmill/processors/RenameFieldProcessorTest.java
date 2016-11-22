package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RenameFieldProcessorTest {
    @Test
    public void testRenameField() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String nestedToField = RandomStringUtils.randomAlphanumeric(5) + "." + RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc(fromField, "value");

        RenameFieldProcessor renameFieldProcessor = new RenameFieldProcessor(fromField, nestedToField, Collections.EMPTY_LIST, true);

        renameFieldProcessor.process(doc);

        assertThat((String)doc.getField(nestedToField)).isEqualTo("value");
        assertThatThrownBy(() -> doc.getField(fromField)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRenameNonExistsFieldAndAddInCaseOfFailure() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String toField = RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc("differentField", "value");

        AddFieldProcessor addFieldProcessor = new AddFieldProcessor(toField, "value");

        RenameFieldProcessor renameFieldProcessor = new RenameFieldProcessor(fromField, toField, Arrays.asList(addFieldProcessor), false);

        renameFieldProcessor.process(doc);

        assertThat((String) doc.getField(toField)).isEqualTo("value");
        assertThatThrownBy(() -> doc.getField(fromField)).isInstanceOf(IllegalStateException.class);
    }
}
