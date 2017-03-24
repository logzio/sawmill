package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static org.assertj.core.api.Assertions.assertThat;

public class CsvProcessorTest {
    @Test
    public void testDefault() {
        String field = "message";
        String csv = "1,\"this\",is,an,ip,\"192.168.1.1\",";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("column1")).isEqualTo("1");
        assertThat((String) doc.getField("column2")).isEqualTo("this");
        assertThat((String) doc.getField("column3")).isEqualTo("is");
        assertThat((String) doc.getField("column4")).isEqualTo("an");
        assertThat((String) doc.getField("column5")).isEqualTo("ip");
        assertThat((String) doc.getField("column6")).isEqualTo("192.168.1.1");
        assertThat((String) doc.getField("column7")).isEqualTo("");
    }

    @Test
    public void testWithColumnsNames() {
        String field = "message";
        String csv = "1,\"this\",is,an,ip,\"192.168.1.1\",true";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("columns", Arrays.asList("id", "field1", "field2", "field3", "field4", "ip", "bool"));
        config.put("convert", ImmutableMap.of("id", "long", "bool", "boolean"));

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((Long) doc.getField("id")).isEqualTo(1l);
        assertThat((String) doc.getField("field1")).isEqualTo("this");
        assertThat((String) doc.getField("field2")).isEqualTo("is");
        assertThat((String) doc.getField("field3")).isEqualTo("an");
        assertThat((String) doc.getField("field4")).isEqualTo("ip");
        assertThat((String) doc.getField("ip")).isEqualTo("192.168.1.1");
        assertThat((boolean) doc.getField("bool")).isEqualTo(true);
    }

    @Test
    public void testWithPartialColumnsNames() {
        String field = "message";
        String csv = "1,\"this\",is,an,ip,\"192.168.1.1\",true";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("columns", Arrays.asList("id", "field1", "field2", "field3", "field4"));

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("id")).isEqualTo("1");
        assertThat((String) doc.getField("field1")).isEqualTo("this");
        assertThat((String) doc.getField("field2")).isEqualTo("is");
        assertThat((String) doc.getField("field3")).isEqualTo("an");
        assertThat((String) doc.getField("field4")).isEqualTo("ip");
        assertThat((String) doc.getField("column6")).isEqualTo("192.168.1.1");
        assertThat((String) doc.getField("column7")).isEqualTo("true");
    }

    @Test
    public void testWithPartialColumnsNamesWithoutAutoGenerateNames() {
        String field = "message";
        String csv = "1,\"this\",is,an,ip,\"192.168.1.1\",true";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("columns", Arrays.asList("id", "field1", "field2", "field3", "field4"));
        config.put("autoGenerateColumnNames", false);

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("id")).isEqualTo("1");
        assertThat((String) doc.getField("field1")).isEqualTo("this");
        assertThat((String) doc.getField("field2")).isEqualTo("is");
        assertThat((String) doc.getField("field3")).isEqualTo("an");
        assertThat((String) doc.getField("field4")).isEqualTo("ip");
        assertThat(doc.hasField("column6")).isFalse();
        assertThat(doc.hasField("column7")).isFalse();
    }

    @Test
    public void testWithSkipEmptyColumns() {
        String field = "message";
        String csv = "1,\"this\",,an,ip,\"192.168.1.1\"";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("skipEmptyColumns", true);

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("column1")).isEqualTo("1");
        assertThat((String) doc.getField("column2")).isEqualTo("this");
        assertThat(doc.hasField("column3")).isFalse();
        assertThat((String) doc.getField("column4")).isEqualTo("an");
        assertThat((String) doc.getField("column5")).isEqualTo("ip");
        assertThat((String) doc.getField("column6")).isEqualTo("192.168.1.1");
    }

    @Test
    public void testWithSeparatorAndQuoteChar() {
        String field = "message";
        String csv = "1\t\\this\\\tis\tan\tip\t\\192.168.1.1\\";

        Doc doc = createDoc(field, csv);

        Map<String,Object> config = new HashMap<>();
        config.put("field", field);
        config.put("separator", "\t");
        config.put("quoteChar", "\\");

        CsvProcessor csvProcessor = new CsvProcessor.Factory().create(config);

        ProcessResult processResult = csvProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField("column1")).isEqualTo("1");
        assertThat((String) doc.getField("column2")).isEqualTo("this");
        assertThat((String) doc.getField("column3")).isEqualTo("is");
        assertThat((String) doc.getField("column4")).isEqualTo("an");
        assertThat((String) doc.getField("column5")).isEqualTo("ip");
        assertThat((String) doc.getField("column6")).isEqualTo("192.168.1.1");
    }
}
