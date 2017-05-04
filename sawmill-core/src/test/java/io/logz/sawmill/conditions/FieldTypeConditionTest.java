package io.logz.sawmill.conditions;


import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utils.DocUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;


public class FieldTypeConditionTest {

    @Test
    public void testWrongConfigs() {
        FieldTypeCondition.Factory factory = new FieldTypeCondition.Factory();
        Map<String,Object> config = new HashMap<>();
        config.put("path", "fieldName");
        config.put("type", "balagan");

        assertThatThrownBy(() ->factory.create(config, null)).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("type [balagan] must be one of");

        config.put("type", "");
        assertThatThrownBy(() ->factory.create(config, null)).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("type [] must be one of");

        assertThatThrownBy(() -> new FieldTypeCondition("path", "not-a-valid-type")).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("type [not-a-valid-type] must be one of");

        assertThatThrownBy(() -> new FieldTypeCondition(null, "not-a-valid-type")).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("failed to parse fieldType condition, could not resolve field path");

        assertThatThrownBy(() -> new FieldTypeCondition("path", null)).isInstanceOf(ProcessorConfigurationException.class)
                .hasMessageContaining("failed to parse fieldType condition, could not resolve field type");




    }

    @Test
    public void testFieldTypeString() {
        FieldTypeCondition isStringCondition = new FieldTypeCondition("testField", "String");

        verifyFalse(isStringCondition, "otherField","otherValue");
        verifyFalse(isStringCondition, "testField", null);
        verifyFalse(isStringCondition, "testField", 5);
        verifyFalse(isStringCondition, "testField", 4.20);
        verifyFalse(isStringCondition, "testField", new HashMap<>());
        verifyFalse(isStringCondition, "testField", new ArrayList<>());
        
        verifyTrue(isStringCondition, "testField", "");
        verifyTrue(isStringCondition, "testField", "some text");
    }

    @Test
    public void testFieldTypeLong() {
        FieldTypeCondition isLongCondition = new FieldTypeCondition("testField", "Long");

        verifyFalse(isLongCondition, "otherField", "otherValue");
        verifyFalse(isLongCondition, "testField", new HashMap<>());
        verifyFalse(isLongCondition, "testField", null);
        verifyFalse(isLongCondition, "testField", new ArrayList<>());
        verifyFalse(isLongCondition, "testField", "");
        verifyFalse(isLongCondition, "testField", "some text");
        verifyFalse(isLongCondition, "testField", "4.20");
        verifyFalse(isLongCondition, "testField", 4.20);
        verifyFalse(isLongCondition, "testField", -4.20);
        verifyFalse(isLongCondition, "testField", "5");

        verifyTrue(isLongCondition, "testField", 5);
        verifyTrue(isLongCondition, "testField", -5);
    }

    @Test
    public void testFieldTypeDouble() {
        FieldTypeCondition isDoubleCondition = new FieldTypeCondition("testField", "double");

        verifyFalse(isDoubleCondition, "otherField","otherValue");
        verifyFalse(isDoubleCondition, "testField", null);
        verifyFalse(isDoubleCondition, "testField", 5);
        verifyFalse(isDoubleCondition, "testField", new HashMap<>());
        verifyFalse(isDoubleCondition, "testField", new ArrayList<>());
        verifyFalse(isDoubleCondition, "testField", "");
        verifyFalse(isDoubleCondition, "testField", "some text");
        verifyFalse(isDoubleCondition, "testField", "4.20");

        verifyTrue(isDoubleCondition, "testField", 4.20);
        verifyTrue(isDoubleCondition, "testField", -4.20);
    }

    @Test
    public void testFieldTypeList() {
        FieldTypeCondition isListCondition = new FieldTypeCondition("testField", "list");

        verifyFalse(isListCondition, "otherField","otherValue");
        verifyFalse(isListCondition, "testField", null);
        verifyFalse(isListCondition, "testField", 5);
        verifyFalse(isListCondition, "testField", new HashMap<>());
        verifyFalse(isListCondition, "testField", "");
        verifyFalse(isListCondition, "testField", "some text");
        verifyFalse(isListCondition, "testField", "4.20");
        verifyFalse(isListCondition, "testField", 4.20);
        verifyFalse(isListCondition, "testField", -4.20);

        verifyTrue(isListCondition, "testField", new ArrayList<>());
    }

    @Test
    public void testFieldTypeJson() {
        FieldTypeCondition isJsonCondition = new FieldTypeCondition("testField", "JsonObject");

        verifyFalse(isJsonCondition, "otherField","otherValue");
        verifyFalse(isJsonCondition, "testField", null);
        verifyFalse(isJsonCondition, "testField", 5);
        verifyFalse(isJsonCondition, "testField", "");
        verifyFalse(isJsonCondition, "testField", "some text");
        verifyFalse(isJsonCondition, "testField", "4.20");
        verifyFalse(isJsonCondition, "testField", 4.20);
        verifyFalse(isJsonCondition, "testField", -4.20);
        verifyFalse(isJsonCondition, "testField", new ArrayList<>());

        verifyTrue(isJsonCondition, "testField", new HashMap<>());
    }

    private void verifyFalse(FieldTypeCondition isTypeCondition, String field, Object value) {
        assertThat(isTypeCondition.evaluate(DocUtils.createDoc(field, value))).isFalse();
    }

    private void verifyTrue(FieldTypeCondition isTypeCondition, String field, Object value) {
        assertThat(isTypeCondition.evaluate(DocUtils.createDoc(field, value))).isTrue();
    }
}
