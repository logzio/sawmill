package io.logz.sawmill;

import io.logz.sawmill.utilities.Grok;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.EMPTY_MAP;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class GrokTest {

    @Test
    public void testNoMatch() throws InterruptedException {
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
    public void testAddPatternByDefinition() throws InterruptedException {
        Map<String, String> bank = new HashMap<>();
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num=[0-9]}%{SINGLEDIGIT:num}");

        String text = "12";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("num");
        assertThat(match.getValues()).isEqualTo(Arrays.asList("1","2"));
    }

    @Test
    public void testMultipleNamedCapturesWithSameName() throws InterruptedException {
        Map<String, String> bank = new HashMap<>();
        bank.put("SINGLEDIGIT", "[0-9]");
        Grok grok = new Grok(bank, "%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}%{SINGLEDIGIT:num}");

        String text = "123";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("num");
        assertThat(match.getValues()).isEqualTo(Arrays.asList("1","2","3"));
    }

    @Test
    public void testNumericCaptures() throws InterruptedException {
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
        assertThat(match1.getValues()).hasSize(1);
        assertThat(match1.getValues()).contains(5009.123d);

        Grok.Match match2 = captures.get(1);
        assertThat(match2.getName()).isEqualTo("bytes");
        assertThat(match2.getValues()).hasSize(1);
        assertThat(match2.getValues()).contains(12009.34d);

        Grok.Match match3 = captures.get(2);
        assertThat(match3.getName()).isEqualTo("status");
        assertThat(match3.getValues()).hasSize(1);
        assertThat(match3.getValues()).contains(200l);

        Grok.Match match4 = captures.get(3);
        assertThat(match4.getName()).isEqualTo("length");
        assertThat(match4.getValues()).hasSize(1);
        assertThat(match4.getValues()).contains(9032l);
    }

    @Test
    public void testNoNamedCaptures() throws InterruptedException {
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
        assertThat(match1.getValues()).hasSize(1);
        assertThat(match1.getValues()).contains("hello");

        Grok.Match match2 = captures.get(1);
        assertThat(match2.getName()).isEqualTo("SPECIAL_NUMBER16");
        assertThat(match2.getValues()).hasSize(1);
        assertThat(match2.getValues()).contains("10000l");

        Grok.Match match3 = captures.get(2);
        assertThat(match3.getName()).isEqualTo("BASE10NUM23");
        assertThat(match3.getValues()).hasSize(1);
        assertThat(match3.getValues()).contains("10000");

        Grok.Match match4 = captures.get(3);
        assertThat(match4.getName()).isEqualTo("BASE10NUM82");
        assertThat(match4.getValues()).hasSize(1);
        assertThat(match4.getValues()).contains("500");

        Grok.Match match5 = captures.get(4);
        assertThat(match5.getName()).isEqualTo("MONTHDAY81");
        assertThat(match5.getValues()).hasSize(1);
        assertThat(match5.getValues()).contains("31");
    }

    @Test
    public void testWithOniguramaNamedCaptures() throws InterruptedException {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo>\\w+)");
        String text = "hello world";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("foo");
        assertThat(match.getValues()).hasSize(1);
        assertThat(match.getValues()).contains("hello");
    }

    @Test
    public void testWithOniguramaWithHyphensNamedCaptures() throws InterruptedException {
        Grok grok = new Grok(EMPTY_MAP, "(?<foo-bar>\\w+)");
        String text = "hello world";

        List<Grok.Match> captures = grok.matches(text);
        assertThat(captures).hasSize(1);

        Grok.Match match = captures.get(0);
        assertThat(match.getName()).isEqualTo("foo-bar");
        assertThat(match.getValues()).hasSize(1);
        assertThat(match.getValues()).contains("hello");
    }

    @Test
    public void testMatchWithoutCaptures() throws InterruptedException {
        String line = "value";
        Grok grok = new Grok(EMPTY_MAP, "value");
        List<Grok.Match> captures = grok.matches(line);
        assertThat(captures).hasSize(0);
    }

    @Test
    public void testMatchInterrupted() {
        Grok grok = new Grok(EMPTY_MAP, ".{10000,}.{100000}");
        String text = RandomStringUtils.random(10000);

        interruptCurrentThreadIn(100);

        assertThatThrownBy(() -> grok.matches(text)).isInstanceOf(InterruptedException.class);
    }

    private void interruptCurrentThreadIn(long millis) {
        Thread currentThread = Thread.currentThread();
        ScheduledExecutorService interrupter = Executors.newScheduledThreadPool(1);
        interrupter.schedule(currentThread::interrupt, millis, MILLISECONDS);
    }

}
