package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Collections;
import java.util.stream.LongStream;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class DropProcessorTest {

    @Test
    public void testDrop() {
        Doc doc = createDoc("drop", "it");

        DropProcessor dropProcessor = createProcessor(DropProcessor.class, Collections.emptyMap());

        ProcessResult processResult = dropProcessor.process(doc);

        assertThat(processResult.isDropped()).isTrue();
    }

    @Test
    public void testDropPercentage() {
        DropProcessor dropProcessor = createProcessor(DropProcessor.class, createConfig("percentage", 90));

        long numberOfDocs = 10;
        long numberOfDroppedDocs = LongStream.range(0, numberOfDocs)
                .mapToObj((i) -> createDoc("drop", "it" + i))
                .map(dropProcessor::process)
                .filter(ProcessResult::isDropped)
                .count();

        assertThat(numberOfDroppedDocs).isGreaterThan(numberOfDocs - numberOfDroppedDocs);
    }
}
