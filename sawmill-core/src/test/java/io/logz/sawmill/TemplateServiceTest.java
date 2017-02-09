package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import com.samskivert.mustache.Template;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.TemplateService.compileTemplate;
import static io.logz.sawmill.TemplateService.compileValue;
import static org.assertj.core.api.Assertions.assertThat;

public class TemplateServiceTest {
    @Test
    public void testCompileTemplate() {
        Template template = compileTemplate("{{foo}}");

        String value = template.execute(ImmutableMap.of("foo", "bar"));

        assertThat(value).isEqualTo("bar");
    }

    @Test
    public void testCompileNullTemplate() {
        assertThat(compileTemplate(null)).isNull();
    }

    @Test
    public void testCompileValueNullTemplate() {
        assertThat(compileValue(null)).isNull();
    }

    @Test
    public void testCompileValueObjectTemplate() {
        Object object = new Object() {
            String str = "something";
            int num = 5;

            @Override
            public String toString() {
                return str + num;
            }
        };
        TemplatedValue template = compileValue(object);

        String value = template.execute(ImmutableMap.of("foo", "bar")).toString();

        assertThat(value).isEqualTo(object.toString());
    }

    @Test
    public void testCompileValueStringTemplate() {
        TemplatedValue template = compileValue("{{foo}}");

        String value = (String) template.execute(ImmutableMap.of("foo", "bar"));

        assertThat(value).isEqualTo("bar");
    }

    @Test
    public void testCompileValueArrayTemplate() {
        TemplatedValue template = compileValue(Arrays.asList("{{foo}}1", 5, ImmutableMap.of("{{foo}}", "{{foo}}2")));

        List value = (List) template.execute(ImmutableMap.of("foo", "bar"));

        assertThat(value.get(0)).isEqualTo("bar1");
        assertThat(value.get(1)).isEqualTo(5);
        assertThat(value.get(2)).isEqualTo(ImmutableMap.of("bar", "bar2"));
    }

    @Test
    public void testCompileValueMapTemplate() {
        TemplatedValue template = compileValue(ImmutableMap.of("{{foo}}NormalMap", "{{foo}}1",
                ImmutableMap.of("map {{foo}}", "as {{foo}}"), ImmutableMap.of("key", "and value {{foo}}")));

        Map value = (Map) template.execute(ImmutableMap.of("foo", "bar"));

        assertThat(value.get("barNormalMap")).isEqualTo("bar1");
        assertThat(value.get("{map bar=as bar}")).isEqualTo(ImmutableMap.of("key", "and value bar"));
    }
}
