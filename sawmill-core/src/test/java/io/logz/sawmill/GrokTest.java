package io.logz.sawmill;

import io.logz.sawmill.utilities.Grok;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.EMPTY_MAP;
import static org.assertj.core.api.Assertions.assertThat;

public class GrokTest {

    @Test
    public void testNoMatch() {
        Map<String, String> bank = new HashMap<>();
        bank.put("MONTHDAY", "(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9])");
        Grok grok = new Grok(bank, "%{MONTHDAY:day}");
        assertThat(grok.captures("nomatch")).isNull();
    }

    @Test
    public void testMultipleNamedCapturesWithSameName() {
        Map<String, String> bank = new HashMap<>();
        bank.put("SINGLEDIGIT", "[0-9]");
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}");

        assertThat(grok.captures("12").get("num")).isEqualTo(Arrays.asList("1","2"));
    }

    @Test
    public void testNumericCaptures() {
        Map<String, String> bank = new HashMap<>();
        bank.put("BASE10NUM", "(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))");
        bank.put("NUMBER", "(?:%{BASE10NUM})");

        String pattern = "%{NUMBER:time:float} %{NUMBER:bytes:double} %{NUMBER:status:int} %{NUMBER:length:long} %{NUMBER}";
        Grok g = new Grok(bank, pattern);

        String text = "5009.123 12009.34 200 9032 1000";
        Map<String, Object> expected = new HashMap<>();
        expected.put("time", 5009.123d);
        expected.put("bytes", 12009.34d);
        expected.put("status", 200l);
        expected.put("length", 9032l);
        Map<String, Object> actual = g.captures(text);

        assertThat(expected).isEqualTo(actual);
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

        assertThat(grok.parsePattern(pattern)).isEqualTo("(?<WORD0>\\w+) - (?<SPECIAL_NUMBER16>(?:(?<BASE10NUM23>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))))l) (?<BASE10NUM82>(?<![0-9.+-])(?>[+-]?(?:(?:[0-9]+(?:\\.[0-9]+)?)|(?:\\.[0-9]+)))) - (?<MONTHDAY81>(?:(?:0[1-9])|(?:[12][0-9])|(?:3[01])|[1-9]))");

        Object actual = grok.captures(text);
        Map<String, Object> expected = new HashMap<>();
        expected.put("WORD0", "hello");
        expected.put("SPECIAL_NUMBER16", "10000l");
        expected.put("BASE10NUM23", "10000");
        expected.put("BASE10NUM82", "500");
        expected.put("MONTHDAY81", "31");
        assertThat(expected).isEqualTo(actual);
    }

    @Test
    public void testWithOniguramaNamedCaptures() {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo>\\w+)");
        Map<String, Object> matches = grok.captures("hello world");
        assertThat(matches.get("foo")).isEqualTo("hello");
    }

    @Test
    public void testWithOniguramaWithHyphensNamedCaptures() {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo-bar>\\w+)");
        Map<String, Object> matches = grok.captures("hello world");
        assertThat(matches.get("foo-bar")).isEqualTo("hello");
    }

    @Test
    public void testMatchWithoutCaptures() {
        String line = "value";
        Grok grok = new Grok(EMPTY_MAP, "value");
        Map<String, Object> matches = grok.captures(line);
        assertThat(matches.size()).isEqualTo(0);
    }

}
