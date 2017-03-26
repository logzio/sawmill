package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;

import static io.logz.sawmill.utils.DocUtils.createDoc;
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
    public void testDocWithAllNeededFields() {
        Doc doc = createDoc(NAME_FIELD, "Robles", SALUD_FIELD, "Buenos Dias");

        String value = template.render(doc);

        assertThat(value).isEqualTo("Buenos Dias señor Robles, Have a good day");
    }

    @Test
    public void testDocWithoutAllFields() {
        Doc doc = createDoc("anotherField", "Robles", SALUD_FIELD, "Buenos Dias");

        String value = template.render(doc);

        assertThat(value).isEqualTo("Buenos Dias señor , Have a good day");
    }

    @Test
    public void testDocWithMapField() {
        Doc doc = createDoc("anotherField", "Robles", SALUD_FIELD, ImmutableMap.of("map", "field"));

        String value = template.render(doc);

        assertThat(value).isEqualTo("{map&#61;field} señor , Have a good day");
    }

    @Test
    public void testDocWithListField() {
        Doc doc = createDoc("anotherField", "Robles", SALUD_FIELD, Arrays.asList("list", "field"));

        String value = template.render(doc);

        assertThat(value).isEqualTo("[list, field] señor , Have a good day");
    }

    @Test
    public void testNullContext() {
        Doc doc = null;

        String value = template.render(doc);

        assertThat(value).isEqualTo(" señor , Have a good day");
    }
}
