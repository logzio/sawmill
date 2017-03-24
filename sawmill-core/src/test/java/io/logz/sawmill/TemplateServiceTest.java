package io.logz.sawmill;

import io.logz.sawmill.exceptions.SawmillException;
import org.junit.Before;
import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public class TemplateServiceTest {
    public TemplateService templateService;

    @Before
    public void init() {
        templateService = new TemplateService();
    }

    @Test
    public void testValidStringTemplate() {
        assertThat(templateService.createTemplate("{{salud}} señor {{name}}, Have a good day")).isInstanceOf(Template.class);
    }

    @Test
    public void testStringWithoutTemplate() {
        assertThat(templateService.createTemplate("no template at all")).isInstanceOf(Template.class);
    }

    @Test
    public void testEmptyString() {
        assertThat(templateService.createTemplate("")).isInstanceOf(Template.class);
    }

    @Test
    public void testNull() {
        assertThatThrownBy(() -> templateService.createTemplate(null)).isInstanceOf(SawmillException.class);
    }
}
