package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ArraysIntersectProcessorTest {
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
        Processor processor = createProcessor(ArraysIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> arrayA = Arrays.asList(1, 2, 3, 3, 4, 5);
        List<Integer> arrayB = Arrays.asList(2, 3, 3, 1);
        Doc doc = createDoc(SOURCE_FIELD_A, arrayA, SOURCE_FIELD_B, arrayB);
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(true);
        Collection<Integer> createdField = doc.getField(TARGET_FIELD);
        assertThat(createdField).hasSameElementsAs(Arrays.asList(1, 2, 3));
    }

    @Test
    public void testEmptyIntersectionAddsEmptyList() throws InterruptedException {
        Processor processor = createProcessor(ArraysIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> arrayA = Arrays.asList(1, 2, 3);
        List<Integer> arrayB = Arrays.asList(4, 5, 6);
        Doc doc = createDoc(SOURCE_FIELD_A, arrayA, SOURCE_FIELD_B, arrayB);
        processor.process(doc);

        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(true);
        Collection<Integer> createdField = doc.getField(TARGET_FIELD);
        assertThat(createdField).hasSameElementsAs(Collections.emptyList());
    }

    @Test
    public void testMissingSourceFieldsFailsProcessing() {
        String badFieldName = "some-other-field";
        Processor processor = createProcessor(ArraysIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        List<Integer> array = Arrays.asList(1, 2, 3);
        Doc doc = createDoc(badFieldName, array, SOURCE_FIELD_B, array);

        assertThatThrownBy(() -> processor.process(doc))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining(SOURCE_FIELD_A);
        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(false);
    }

    @Test
    public void testSourceFieldIsNotIterableFailsProcessing() {
        Processor processor = createProcessor(ArraysIntersectProcessor.class, LIST_INTERSECT_CONFIG);
        String notIterable = "not-iterable-at-all";
        Doc doc = createDoc(SOURCE_FIELD_A, notIterable, SOURCE_FIELD_B, notIterable);

        assertThatThrownBy(() -> processor.process(doc)).isInstanceOf(ClassCastException.class);
        assertThat(doc.hasField(TARGET_FIELD)).isEqualTo(false);
    }
}
