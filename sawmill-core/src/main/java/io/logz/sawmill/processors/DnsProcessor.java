package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "dns", factory = DnsProcessor.Factory.class)
public class DnsProcessor implements Processor {
    private static final Logger logger = LoggerFactory.getLogger(DnsProcessor.class);

    private final String action;
    private final List<String> resolve;
    private final List<String> reverse;

    public DnsProcessor(String action,
                        List<String> resolve,
                        List<String> reverse) {
        this.action = action;
        this.resolve = resolve;
        this.reverse = reverse;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Map<String, String> afterDns = new HashMap<>();

        if (CollectionUtils.isNotEmpty(resolve)) {
            resolve.forEach(field -> {
                if (doc.hasField(field, String.class)) {
                    String ip = resolve(doc.getField(field));
                    if (StringUtils.isNotEmpty(ip)) {
                        afterDns.put(field, ip);
                    }
                }
            });
        }

        if (CollectionUtils.isNotEmpty(reverse)) {
            reverse.forEach(field -> {
                if (doc.hasField(field, String.class)) {
                    String host = reverse(doc.getField(field));
                    if (StringUtils.isNotEmpty(host)) {
                        afterDns.put(field, host);
                    }
                }
            });
        }

        if (action.equals("append")) {
            afterDns.forEach(doc::appendList);
        } else if (action.equals("replace")) {
            afterDns.forEach(doc::addField);
        }

        return ProcessResult.success();
    }

    private String reverse(String ip) {
        try {
            InetAddress inetAddress = InetAddress.getByName(ip);
            return inetAddress.getHostName();
        } catch (UnknownHostException e) {
            logger.warn("couldn't reverse the ip [{}]", ip, e);
        }

        return null;
    }

    private String resolve(String hostName) {
        try {
            InetAddress inetAddress = InetAddress.getByName(hostName);
            return inetAddress.getHostAddress();
        } catch (UnknownHostException e) {
            logger.warn("couldn't resolve the hostName [{}]", hostName, e);
        }

        return null;
    }

    public static class Factory implements Processor.Factory {

        @Override
        public DnsProcessor create(Map<String,Object> config) {
            DnsProcessor.Configuration dnsConfig = JsonUtils.fromJsonMap(DnsProcessor.Configuration.class, config);

            return new DnsProcessor(dnsConfig.getAction(),
                    dnsConfig.getResolve(),
                    dnsConfig.getReverse());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String action = "append";
        private List<String> resolve;
        private List<String> reverse;

        public Configuration() { }

        public String getAction() {
            return action;
        }

        public List<String> getResolve() {
            return resolve;
        }

        public List<String> getReverse() {
            return reverse;
        }
    }
}
