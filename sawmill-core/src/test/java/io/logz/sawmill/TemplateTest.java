package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TemplateTest {
    public static final String NAME_FIELD = "name";
    public static final String SALUD_FIELD = "salud";
    public static final String DATE_FIELD = "date";
    public static Template template;

    @BeforeClass
    public static void init() {
        template = new TemplateService().createTemplate("{{" + SALUD_FIELD + "}} señor {{" + NAME_FIELD + "}}, Have a good day{{" + DATE_FIELD + "}}");
    }

    @Test
    public void testDocWithAllNeededFields() {
        Doc doc = createDoc(NAME_FIELD, "Robles", SALUD_FIELD, "Buenos Dias", DATE_FIELD, ", today");

        String value = template.render(doc);

        assertThat(value).isEqualTo("Buenos Dias señor Robles, Have a good day, today");
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

    @Test
    public void testDateTemplate() {
        String dateFormat = "dd.mm.yyyy";
        Template template = new TemplateService().createTemplate("Today is {{#dateTemplate}}" + dateFormat + "{{/dateTemplate}}");
        Doc doc = createDoc("field1", "value1");

        String expectedDate =  DateTimeFormatter.ofPattern(dateFormat).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC));
        assertThat(template.render(doc)).isEqualTo("Today is " + expectedDate);
    }

    @Test
    public void testInvalidDateTemplate() {
        String dateFormat = "hello";
        Template template = new TemplateService().createTemplate("Today is {{#dateTemplate}}" + dateFormat + "{{/dateTemplate}}");
        Doc doc = createDoc("field1", "value1");

        assertThatThrownBy(() -> template.render(doc)).isInstanceOf(IllegalArgumentException.class);
    }
}
