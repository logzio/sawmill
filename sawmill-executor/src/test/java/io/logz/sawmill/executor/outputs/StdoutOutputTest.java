package io.logz.sawmill.executor.outputs;

import io.logz.sawmill.Doc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static io.logz.sawmill.executor.utils.DocUtils.createDoc;
import static java.util.Collections.EMPTY_MAP;
import static org.assertj.core.api.Assertions.assertThat;

public class StdoutOutputTest {
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void cleanUpStreams() {
        System.setOut(null);
    }

    @Test
    public void testPrintToConsole() {
        StdoutOutput stdoutOutput = new StdoutOutput.Factory().create(EMPTY_MAP);

        Doc doc1 = createDoc("field1", "value1");
        Doc doc2 = createDoc("field2", "value2");
        Doc doc3 = createDoc("field3", "value3");

        List<Doc> docs = Arrays.asList(doc1, doc2, doc3);
        stdoutOutput.send(docs);

        String expectedOutput = String.join("\n", docs.stream()
                .map(doc -> doc.getSource().toString())
                .collect(Collectors.toList())) + "\n";
        assertThat(expectedOutput)
                .isEqualTo(outContent.toString());
    }


}
