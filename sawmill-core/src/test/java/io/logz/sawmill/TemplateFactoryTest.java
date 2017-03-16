package io.logz.sawmill;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Arrays;
import java.util.Optional;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class TemplateFactoryTest {
    @Test
    public void testValidStringTemplate() {
        Optional<Template> template = TemplateFactory.compileTemplate("{{salud}} señor {{name}}, Have a good day");
        assertThat(template.isPresent()).isTrue();
    }

    @Test
    public void testStringWithoutTemplate() {
        assertThat(TemplateFactory.compileTemplate("no template at all").isPresent()).isFalse();
    }

    @Test
    public void testList() {
        assertThat(TemplateFactory.compileTemplate(Arrays.asList("this", "is", "not", "string")).isPresent()).isFalse();
    }

    @Test
    public void testMap() {
        assertThat(TemplateFactory.compileTemplate(ImmutableMap.of("neither", "this")).isPresent()).isFalse();
    }

    @Test
    public void testNumber() {
        assertThat(TemplateFactory.compileTemplate(1).isPresent()).isFalse();
    }

    @Test
    public void testBoolean() {
        assertThat(TemplateFactory.compileTemplate(false).isPresent()).isFalse();
    }

    @Test
    public void testBytes() {
        assertThat(TemplateFactory.compileTemplate("some bytes".getBytes()).isPresent()).isFalse();
    }

    @Test
    public void testNull() {
        assertThat(TemplateFactory.compileTemplate(null).isPresent()).isFalse();
    }
}
