package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TemplateTest {
    public static final String NAME_FIELD = "name";
    public static final String SALUD_FIELD = "salud";
    public static Template template;

    @BeforeClass
    public static void init() {
        template = new TemplateService().createTemplate("{{" + SALUD_FIELD + "}} señor {{" + NAME_FIELD + "}}, Have a good day");
    }

    @Test
    public void testMapContextWithAllNeededFields() {
        Object context = ImmutableMap.of(NAME_FIELD, "Robles", SALUD_FIELD, "Buenos Dias");

        String value = template.render(context);

        assertThat(value).isEqualTo("Buenos Dias señor Robles, Have a good day");
    }

    @Test
    public void testMapContextWithoutAllFieldsFields() {
        Object context = ImmutableMap.of("anotherField", "Robles", SALUD_FIELD, "Buenos Dias");

        String value = template.render(context);

        assertThat(value).isEqualTo("Buenos Dias señor , Have a good day");
    }

    @Test
    public void testNullContext() {
        Object context = null;

        String value = template.render(context);

        assertThat(value).isEqualTo(" señor , Have a good day");
    }

    @Test
    public void testListContext() {
        Object context = Arrays.asList(NAME_FIELD, "Robles", SALUD_FIELD, "Buenos");

        String value = template.render(context);

        assertThat(value).isEqualTo(" señor , Have a good day");
    }

    @Test
    public void testClassContext() {
        Object context = new Object() {
            String name = "Robles";
            String salud = "Buenos Dias";
        };

        String value = template.render(context);

        assertThat(value).isEqualTo("Buenos Dias señor Robles, Have a good day");
    }
}
