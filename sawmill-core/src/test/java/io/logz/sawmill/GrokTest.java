package io.logz.sawmill;

import io.logz.sawmill.utilities.Grok;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.EMPTY_MAP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrokTest {

    @Test
    public void testNoMatch() {
        Map<String, String> bank = new HashMap<>();
        bank.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        Grok grok = new Grok(bank, "%{MONTHDAY:day}");
        assertThat(grok.matches("nomatch")).isNull();
    }

    @Test
    public void testUnknownPattern() {
        Map<String, String> bank = new HashMap<>();
        assertThatThrownBy(() -> new Grok(bank, "%{NONEXISTSPATTERN}")).isInstanceOf(RuntimeException.class);
    }

    @Test
    public void testAddPatternByDefinition() {
        Map<String, String> bank = new HashMap<>();
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num=[0-9]}%{SINGLEDIGIT:num}");

        String text = "12";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("num");
        assertThat(match.getValue()).isEqualTo(Arrays.asList("1","2"));
        assertThat(text.substring(match.getStart(), match.getEnd())).isEqualTo("12");
    }

    @Test
    public void testMultipleNamedCapturesWithSameName() {
        Map<String, String> bank = new HashMap<>();
        bank.put("SINGLEDIGIT", "[0-9]");
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}");

        String text = "123";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("num");
        assertThat(match.getValue()).isEqualTo(Arrays.asList("1","2","3"));
        assertThat(text.substring(match.getStart(), match.getEnd())).isEqualTo("123");
    }

    @Test
    public void testNumericCaptures() {
        Map<String, String> bank = new HashMap<>();
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:time:float} %{NUMBER:bytes:double} %{NUMBER:status:int} %{NUMBER:length:long} %{NUMBER}";
        Grok g = new Grok(bank, pattern);

        String text = "5009.123 12009.34 200 9032 1000";

        List<Grok.Match> captures = g.matches(text);
        assertThat(captures).hasSize(4);

        Grok.Match match1 = captures.get(0);
        assertThat(match1.getName()).isEqualTo("time");
        assertThat(match1.getValue()).isEqualTo(5009.123d);
        assertThat(text.substring(match1.getStart(), match1.getEnd())).isEqualTo("5009.123");

        Grok.Match match2 = captures.get(1);
        assertThat(match2.getName()).isEqualTo("bytes");
        assertThat(match2.getValue()).isEqualTo(12009.34d);
        assertThat(text.substring(match2.getStart(), match2.getEnd())).isEqualTo("12009.34");

        Grok.Match match3 = captures.get(2);
        assertThat(match3.getName()).isEqualTo("status");
        assertThat(match3.getValue()).isEqualTo(200l);
        assertThat(text.substring(match3.getStart(), match3.getEnd())).isEqualTo("200");

        Grok.Match match4 = captures.get(3);
        assertThat(match4.getName()).isEqualTo("length");
        assertThat(match4.getValue()).isEqualTo(9032l);
        assertThat(text.substring(match4.getStart(), match4.getEnd())).isEqualTo("9032");
    }

    @Test
    public void testNoNamedCaptures() {
        Map<String, String> bank = new HashMap<>();

        bank.put("WORD", "\\w+");
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("SPECIAL_NUMBER", "(?:%{BASE10NUM})l");
        bank.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");

        String text = "hello - 10000l 500 - 31";
        String pattern = "%{WORD} - %{SPECIAL_NUMBER} %{BASE10NUM} - %{MONTHDAY}";
        Grok grok = new Grok(bank, pattern, false);

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(5);

        Grok.Match match1 = captures.get(0);
        assertThat(match1.getName()).isEqualTo("WORD0");
        assertThat(match1.getValue()).isEqualTo("hello");
        assertThat(text.substring(match1.getStart(), match1.getEnd())).isEqualTo("hello");

        Grok.Match match2 = captures.get(1);
        assertThat(match2.getName()).isEqualTo("SPECIAL_NUMBER16");
        assertThat(match2.getValue()).isEqualTo("10000l");
        assertThat(text.substring(match2.getStart(), match2.getEnd())).isEqualTo("10000l");

        Grok.Match match3 = captures.get(2);
        assertThat(match3.getName()).isEqualTo("BASE10NUM23");
        assertThat(match3.getValue()).isEqualTo("10000");
        assertThat(text.substring(match3.getStart(), match3.getEnd())).isEqualTo("10000");

        Grok.Match match4 = captures.get(3);
        assertThat(match4.getName()).isEqualTo("BASE10NUM82");
        assertThat(match4.getValue()).isEqualTo("500");
        assertThat(text.substring(match4.getStart(), match4.getEnd())).isEqualTo("500");

        Grok.Match match5 = captures.get(4);
        assertThat(match5.getName()).isEqualTo("MONTHDAY81");
        assertThat(match5.getValue()).isEqualTo("31");
        assertThat(text.substring(match5.getStart(), match5.getEnd())).isEqualTo("31");
    }

    @Test
    public void testWithOniguramaNamedCaptures() {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo>\\w+)");
        String text = "hello world";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("foo");
        assertThat(match.getValue()).isEqualTo("hello");
        assertThat(text.substring(match.getStart(), match.getEnd())).isEqualTo("hello");
    }

    @Test
    public void testWithOniguramaWithHyphensNamedCaptures() {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo-bar>\\w+)");
        String text = "hello world";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("foo-bar");
        assertThat(match.getValue()).isEqualTo("hello");
        assertThat(text.substring(match.getStart(), match.getEnd())).isEqualTo("hello");
    }

    @Test
    public void testMatchWithoutCaptures() {
        String line = "value";
        Grok grok = new Grok(EMPTY_MAP, "value");
        List<Grok.Match> captures = grok.matches(line);
        assertThat(captures).hasSize(0);
    }

}
