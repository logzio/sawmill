package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ListIntersectProcessorTest {
    private static final String SOURCE_FIELD_A = "source-a-field";
    private static final String SOURCE_FIELD_B = "source-b-field";
    private static final String TARGET_FIELD = "target-field";
    private static final Map<String, Object> LIST_INTERSECT_CONFIG = ImmutableMap.of(
            "sourceFieldA", SOURCE_FIELD_A,
            "sourceFieldB", SOURCE_FIELD_B,
            "targetField", TARGET_FIELD
    );

    @Test
    public void testSimpleIntersection() throws InterruptedException {
        Processor processor = createProcessor(ListIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> listA = Arrays.asList(1, 2, 3, 3, 4, 5);
        List<Integer> listB = Arrays.asList(2, 3, 3, 1);
        Doc doc = createDoc(SOURCE_FIELD_A, listA, SOURCE_FIELD_B, listB);
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(true);
        Collection<Integer> createdField = doc.getField(TARGET_FIELD);
        assertThat(createdField).hasSameElementsAs(Arrays.asList(1, 2, 3));
    }

    @Test
    public void testEmptyIntersectionDoesNotAddField() throws InterruptedException {
        Processor processor = createProcessor(ListIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> listA = Arrays.asList(1, 2, 3);
        List<Integer> listB = Arrays.asList(4, 5, 6);
        Doc doc = createDoc(SOURCE_FIELD_A, listA, SOURCE_FIELD_B, listB);
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(false);
    }

    @Test
    public void testMissingSourceFieldsFailProcessing() {
        String badFieldName = "some-other-field";
        Processor processor = createProcessor(ListIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> list = Arrays.asList(1, 2, 3);
        Doc doc = createDoc(badFieldName, list, SOURCE_FIELD_B, list);

        assertThatThrownBy(() -> processor.process(doc))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SOURCE_FIELD_A);
    }
}
