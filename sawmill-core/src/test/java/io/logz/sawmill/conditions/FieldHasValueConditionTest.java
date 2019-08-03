package io.logz.sawmill.conditions;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Doubles;
import io.logz.sawmill.Doc;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createCondition;
import static org.assertj.core.api.Assertions.assertThat;

public class FieldHasValueConditionTest {
    private String field = "field1";

    @Test
    public void testEmptyPossibleValues() {
        List<Object> possibleValues = Collections.emptyList();
        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(field, possibleValues);

        Doc doc = createDoc(field, "value1");
        assertThat(fieldHasValueCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldNotExists() {
        List<Object> possibleValues = Collections.singletonList("value1");
        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(field, possibleValues);

        Doc doc = createDoc("field2", "value2");
        assertThat(fieldHasValueCondition.evaluate(doc)).isFalse();
    }

    @Test
    public void testFieldHasValue() {
        String stringValue = "value1";
        Integer intValue = 1;
        long longValue = 2L;
        Float floatValue = 4.7f;
        double doubleValue = 3.5d;
        List<String> listValue = Arrays.asList("some", "list");
        ImmutableMap<String, String> mapValue = ImmutableMap.of("some", "map");
        String templateValue = "{{templateField}}";
        List<Object> possibleValues = Arrays.asList(stringValue, intValue, longValue, floatValue, doubleValue, true, listValue, mapValue, templateValue);
        FieldHasValueCondition fieldHasValueCondition = createCondition(FieldHasValueCondition.class, "field", field, "possibleValues", possibleValues);

        Doc doc = createDoc(field, stringValue);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, intValue.longValue());
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, longValue);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, Doubles.tryParse(floatValue.toString()));
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, doubleValue);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, true);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, listValue);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, mapValue);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();

        doc = createDoc(field, "templateValue",
                "templateField", "templateValue");
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testIntegerEvaluation() {
        String randomField = RandomStringUtils.randomAlphabetic(10);
        int randomValue = ThreadLocalRandom.current().nextInt();
        Doc doc = createDoc(randomField, randomValue);
        List<Object> possibleValues = getRandomLongsList(randomValue);

        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(randomField, possibleValues);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();
    }

    @Test
    public void testFloatEvaluation() {
        String randomField = RandomStringUtils.randomAlphabetic(10);
        float randomValue = ThreadLocalRandom.current().nextFloat();
        List<Object> possibleValues = getRandomDoublesList(randomValue);
        Doc doc = createDoc(randomField, randomValue);

        FieldHasValueCondition fieldHasValueCondition = new FieldHasValueCondition(randomField, possibleValues);
        assertThat(fieldHasValueCondition.evaluate(doc)).isTrue();
    }

    private List<Object> getRandomLongsList(int randomValue) {
        List<Object> list = new ArrayList<>();

        IntStream.range(0, 10)
                .forEach(i -> {
                    long randomNumber = ThreadLocalRandom.current().nextLong();
                    list.add(randomNumber);
                });
        list.add((long)randomValue);
        return list;
    }

    private List<Object> getRandomDoublesList(float randomValue) {
        List<Object> list = new ArrayList<>();

        IntStream.range(0, 10)
                .forEach(i -> {
                    Double randomNumber = ThreadLocalRandom.current().nextDouble();
                    list.add(randomNumber);
                });
        list.add((double)randomValue);
        return list;
    }
}