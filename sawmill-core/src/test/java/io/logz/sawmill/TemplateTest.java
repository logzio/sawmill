package io.logz.sawmill;

import com.github.mustachejava.MustacheException;
import com.google.common.collect.ImmutableMap;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static io.logz.sawmill.TemplateService.JSON_STRING_SUFFIX;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TemplateTest {
    public static final String NAME_FIELD = "name";
    public static final String SALUD_FIELD = "salud";
    // TODO: change it to date after removing compatibility
    public static final String DATE_FIELD = "someDate";
    public static Template template;
    private static TemplateService templateService;

    @BeforeClass
    public static void init() {
        templateService = new TemplateService();
        template = templateService.createTemplate("{{" + SALUD_FIELD + "}} señor {{" + NAME_FIELD + "}}, Have a good day{{" + DATE_FIELD + "}}");
    }

    @Test
    public void testDocWithAllNeededFields() {
        Doc doc = createDoc(NAME_FIELD, "'Robles'", SALUD_FIELD, "Buenos Dias", DATE_FIELD, ", today");

        String value = template.render(doc);

        assertThat(value).isEqualTo("Buenos Dias señor 'Robles', Have a good day, today");
    }

    @Test
    public void testDocWithoutAllFields() {
        Doc doc = createDoc("anotherField", "Robles", SALUD_FIELD, "Buenos Dias");

        String value = template.render(doc);

        assertThat(value).isEqualTo("Buenos Dias señor , Have a good day");
    }

    @Test
    public void testDocWithMapField() {
        Template mapTemplate = templateService.createTemplate("this is {{map}} and this is specific field {{map.field1}} and non exists one {{map.nonExistsField}}");
        Doc doc = createDoc("map", ImmutableMap.of("field1", "value1", "field2", "value2"));

        String value = mapTemplate.render(doc);

        assertThat(value).isEqualTo("this is {field1=value1, field2=value2} and this is specific field value1 and non exists one ");
    }

    @Test
    public void testDocWithJsonMapField() {
        Template mapTemplate = templateService.createTemplate("this is the json {{map" + JSON_STRING_SUFFIX + "}} and this is specific field {{map.field1}} and non exists one {{map.nonExistsField}}");
        Doc doc = createDoc("map", ImmutableMap.of("field1", "value1", "field2", "value2"));

        String value = mapTemplate.render(doc);

        assertThat(value).isEqualTo("this is the json {\"field1\":\"value1\",\"field2\":\"value2\"} and this is specific field value1 and non exists one ");
    }

    @Test
    public void testDocWithListField() {
        Template listTemplate = templateService.createTemplate("this is {{list}} and this is first {{list.first}}, specific index {{list.1}} and last {{list.last}}");
        Doc doc = createDoc("list", Arrays.asList("index0", "index1", "index3"));

        String value = listTemplate.render(doc);

        assertThat(value).isEqualTo("this is {0=index0, 1=index1, 2=index3, last=index3, first=index0} and this is first index0, specific index index1 and last index3");
    }

    @Test
    public void testNullContext() {
        Doc doc = null;

        String value = template.render(doc);

        assertThat(value).isEqualTo(" señor , Have a good day");
    }

    @Test
    public void testInvalidAccessWithMustachePartials() {
        assertThatThrownBy(() -> new TemplateService().createTemplate("This is my host file content:\n {{>/etc/hosts}}"))
                .isInstanceOf(MustacheException.class)
                .hasMessageContaining("Disallowed: resource requested");
    }

    @Test
    public void testDateTemplate() {
        String dateFormat = "dd.MM.yyyy";
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

        assertThatThrownBy(() -> template.render(doc)).isInstanceOf(MustacheException.class)
                                                      .hasCauseExactlyInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testCompatibilityDateTemplate() {
        String dateFormat = "dd.MM.yyyy";
        Template template = new TemplateService().createTemplate("Today is {{#date}}" + dateFormat + "{{/date}}");
        Doc doc = createDoc("field1", "value1");

        String expectedDate =  DateTimeFormatter.ofPattern(dateFormat).format(ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneOffset.UTC));
        assertThat(template.render(doc)).isEqualTo("Today is " + expectedDate);
    }
}
