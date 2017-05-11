package io.logz.sawmill.executor.inputs;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FileInputTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private File singleLog;
    private File multipleLogs;

    private File singleJsonLog;
    private File multipleJsonLogs;

    @Before
    public void init() throws IOException {
        List<String> plainLogs = Arrays.asList("first line", "second line", "third line");
        Map<String, String> log1 = ImmutableMap.of("message", "first line");
        Map<String, String> log2 = ImmutableMap.of("message", "second line");
        Map<String, String> log3 = ImmutableMap.of("message", "third line");
        List<String> jsonLogs = Arrays.asList(JsonUtils.toJsonString(log1), JsonUtils.toJsonString(log2), JsonUtils.toJsonString(log3));

        singleLog = folder.newFile("singleLog.txt");
        Files.write(singleLog.toPath(), plainLogs.subList(0,1), Charset.forName("UTF-8"));

        multipleLogs = folder.newFile("multipleLogs.txt");
        Files.write(multipleLogs.toPath(), plainLogs, Charset.forName("UTF-8"));

        singleJsonLog = folder.newFile("singleJsonLog.json");
        Files.write(singleJsonLog.toPath(), jsonLogs.subList(0,1), Charset.forName("UTF-8"));

        multipleJsonLogs = folder.newFile("multipleJsonLogs.json");
        Files.write(multipleJsonLogs.toPath(), jsonLogs, Charset.forName("UTF-8"));
    }

    @Test
    public void testEmptyPath() {
        Map<String, Object> config = new HashMap<>();
        config.put("path", null);
        assertThatThrownBy(() -> new FileInput.Factory().create(config)).isInstanceOf(IllegalStateException.class);

        config.put("path", "");
        assertThatThrownBy(() -> new FileInput.Factory().create(config)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testFileWithOneLog() throws IOException{
        Map<String, Object> config = new HashMap<>();
        config.put("path", singleLog.getAbsolutePath());

        FileInput fileInput = new FileInput.Factory().create(config);

        List<Doc> docs = fileInput.listen();

        assertThat(docs.size()).isEqualTo(1);
        assertThat(docs.get(0).getSource()).isEqualTo(ImmutableMap.of("message", "first line"));
    }

    @Test
    public void testFileWithSeveralLogs() throws IOException{
        Map<String, Object> config = new HashMap<>();
        config.put("path", multipleLogs.getAbsolutePath());

        FileInput fileInput = new FileInput.Factory().create(config);

        List<Doc> docs = fileInput.listen();

        assertThat(docs.size()).isEqualTo(3);
        assertThat(docs.get(0).getSource()).isEqualTo(ImmutableMap.of("message", "first line"));
        assertThat(docs.get(1).getSource()).isEqualTo(ImmutableMap.of("message", "second line"));
        assertThat(docs.get(2).getSource()).isEqualTo(ImmutableMap.of("message", "third line"));
    }

    @Test
    public void testJsonFileWithOneLog() throws IOException{
        Map<String, Object> config = new HashMap<>();
        config.put("path", singleJsonLog.getAbsolutePath());
        config.put("codec", "json");

        FileInput fileInput = new FileInput.Factory().create(config);

        List<Doc> docs = fileInput.listen();

        assertThat(docs.size()).isEqualTo(1);
        assertThat(docs.get(0).getSource()).isEqualTo(ImmutableMap.of("message", "first line"));
    }

    @Test
    public void testJsonFileWithSeveralLogs() throws IOException{
        Map<String, Object> config = new HashMap<>();
        config.put("path", multipleJsonLogs.getAbsolutePath());
        config.put("codec", "json");

        FileInput fileInput = new FileInput.Factory().create(config);

        List<Doc> docs = fileInput.listen();

        assertThat(docs.size()).isEqualTo(3);
        assertThat(docs.get(0).getSource()).isEqualTo(ImmutableMap.of("message", "first line"));
        assertThat(docs.get(1).getSource()).isEqualTo(ImmutableMap.of("message", "second line"));
        assertThat(docs.get(2).getSource()).isEqualTo(ImmutableMap.of("message", "third line"));
    }
}
