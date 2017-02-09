package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RenameFieldProcessorTest {
    @Test
    public void testRenameField() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String nestedToField = RandomStringUtils.randomAlphanumeric(5) + "." + RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc(fromField, "value");

        Map<String, Object> config = createConfig("from", fromField, "to", nestedToField);

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isTrue();

        assertThat((String)doc.getField(nestedToField)).isEqualTo("value");
        assertThatThrownBy(() -> doc.getField(fromField)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testRenameNonExistsField() {
        String fromField = RandomStringUtils.randomAlphanumeric(5);
        String toField = RandomStringUtils.randomAlphanumeric(5);
        Doc doc = createDoc("differentField", "value");

        Map<String, Object> config = createConfig("from", fromField, "to", toField);

        RenameFieldProcessor renameFieldProcessor = createProcessor(RenameFieldProcessor.class, config);

        assertThat(renameFieldProcessor.process(doc).isSucceeded()).isFalse();

        assertThatThrownBy(() -> doc.getField(toField)).isInstanceOf(IllegalStateException.class);
    }
}
