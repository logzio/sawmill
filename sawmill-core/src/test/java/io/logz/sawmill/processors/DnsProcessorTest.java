package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

public class DnsProcessorTest {

    @Test
    public void testResolveSeveralHostsAppend() throws UnknownHostException{
        String field1 = "host1";
        String field2 = "host2";
        String field3 = "host3";

        String host1 = "google.com";
        String host2 = "logz.io";

        String ip1 = InetAddress.getByName(host1).getHostAddress();
        String ip2 = InetAddress.getByName(host2).getHostAddress();

        Doc doc = createDoc(field1, host1,
                field2, host2,
                field3, "invalidhost");

        Map<String, Object> config = createConfig("resolve", Arrays.asList(field1, field2, field3));

        DnsProcessor dnsProcessor = createProcessor(DnsProcessor.class, config);

        ProcessResult processResult = dnsProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List) doc.getField(field1)).isEqualTo(Arrays.asList(host1, ip1));
        assertThat((List) doc.getField(field2)).isEqualTo(Arrays.asList(host2, ip2));
        assertThat((String) doc.getField(field3)).isEqualTo("invalidhost");
    }

    @Test
    public void testResolveSeveralHostsReplace() throws UnknownHostException{
        String field1 = "host1";
        String field2 = "host2";
        String field3 = "host3";

        String host1 = "google.com";
        String host2 = "logz.io";

        String ip1 = InetAddress.getByName(host1).getHostAddress();
        String ip2 = InetAddress.getByName(host2).getHostAddress();

        Doc doc = createDoc(field1, host1,
                field2, host2,
                field3, "invalidhost");

        Map<String, Object> config = createConfig("resolve", Arrays.asList(field1, field2, field3),
                "action", "replace");

        DnsProcessor dnsProcessor = createProcessor(DnsProcessor.class, config);

        ProcessResult processResult = dnsProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field1)).isEqualTo(ip1);
        assertThat((String) doc.getField(field2)).isEqualTo(ip2);
        assertThat((String) doc.getField(field3)).isEqualTo("invalidhost");
    }

    @Test
    public void testReverseSeveralHostsAppend() throws UnknownHostException{
        String field1 = "ip1";
        String field2 = "ip2";
        String field3 = "ip3";

        String ip1 = "8.8.8.8";
        String ip2 = "104.130.102.37";

        String host1 = InetAddress.getByName(ip1).getHostName();
        String host2 = InetAddress.getByName(ip2).getHostName();

        Doc doc = createDoc(field1, ip1,
                field2, ip2,
                field3, "not.ip");

        Map<String, Object> config = createConfig("reverse", Arrays.asList(field1, field2, field3));

        DnsProcessor dnsProcessor = createProcessor(DnsProcessor.class, config);

        ProcessResult processResult = dnsProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((List) doc.getField(field1)).isEqualTo(Arrays.asList(ip1, host1));
        assertThat((List) doc.getField(field2)).isEqualTo(Arrays.asList(ip2, host2));
        assertThat((String) doc.getField(field3)).isEqualTo("not.ip");
    }

    @Test
    public void testReverseSeveralHostsReplace() throws UnknownHostException{
        String field1 = "ip1";
        String field2 = "ip2";
        String field3 = "ip3";


        String ip1 = "8.8.8.8";
        String ip2 = "104.130.102.37";

        String host1 = InetAddress.getByName(ip1).getHostName();
        String host2 = InetAddress.getByName(ip2).getHostName();

        Doc doc = createDoc(field1, ip1,
                field2, ip2,
                field3, "not.ip");

        Map<String, Object> config = createConfig("reverse", Arrays.asList(field1, field2, field3),
                "action", "replace");

        DnsProcessor dnsProcessor = createProcessor(DnsProcessor.class, config);

        ProcessResult processResult = dnsProcessor.process(doc);

        assertThat(processResult.isSucceeded()).isTrue();
        assertThat((String) doc.getField(field1)).isEqualTo(host1);
        assertThat((String) doc.getField(field2)).isEqualTo(host2);
        assertThat((String) doc.getField(field3)).isEqualTo("not.ip");
    }
}
