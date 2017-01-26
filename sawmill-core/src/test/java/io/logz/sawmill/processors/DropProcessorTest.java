package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.apache.commons.collections.MapUtils;
import org.junit.Test;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class DropProcessorTest {
    @Test
    public void testDrop() {
        Doc doc = createDoc("drop", "it");

        DropProcessor dropProcessor = new DropProcessor.Factory().create(MapUtils.EMPTY_MAP);

        ProcessResult processResult = dropProcessor.process(doc);

        assertThat(processResult.isDropped()).isTrue();
    }
}
