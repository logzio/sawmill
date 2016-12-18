package io.logz.sawmill.processors;

import com.google.common.io.Resources;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import ua_parser.CachingParser;
import ua_parser.Client;
import ua_parser.OS;
import ua_parser.Parser;
import ua_parser.UserAgent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@ProcessorProvider(type = "userAgent", factory = UserAgentProcessor.Factory.class)
public class UserAgentProcessor implements Processor {
    private final String field;
    private final String targetField;
    private final Parser uaParser;

    public UserAgentProcessor(String field, String targetField, Parser uaParser) {
        this.field = field;
        this.targetField = targetField;
        this.uaParser = uaParser;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(String.format("failed to parse user agent, couldn't find field [%s]", field));
        }

        String uaString = doc.getField(field);

        Client client = uaParser.parse(uaString);

        Map<String, String> userAgent = new HashMap<>();
        if (client.userAgent != null) {
            setUserAgentProperties(client.userAgent, userAgent);
        }
        else {
            userAgent.put("name", "Other");
        }

        if (client.os != null) {
            setOsProperties(client.os, userAgent);
        } else {
            userAgent.put("os", "Other");
        }

        if (client.device != null) {
            userAgent.put("device", client.device.family);
        } else {
            userAgent.put("device", "Other");
        }

        if (targetField != null) {
            doc.addField(targetField, userAgent);
        } else {
            userAgent.entrySet().forEach(property -> {
                doc.addField(property.getKey(), property.getValue());
            });
        }

        return ProcessResult.success();
    }

    private void setOsProperties(OS os, Map<String, String> userAgent) {
        StringBuilder fullOSName = new StringBuilder(os.family);

        userAgent.put("os_name", os.family);

        if (os.major != null) {
            userAgent.put("os_major", os.major);
            fullOSName.append(" ");
            fullOSName.append(os.major);
        }
        if (os.minor != null) {
            userAgent.put("os_minor", os.minor);
            fullOSName.append(".");
            fullOSName.append(os.minor);
        }
        if (os.patch != null) {
            userAgent.put("os_patch", os.patch);
            fullOSName.append(".");
            fullOSName.append(os.patch);
        }
        if (os.patchMinor != null) {
            userAgent.put("os_build", os.patchMinor);
            fullOSName.append(".");
            fullOSName.append(os.patchMinor);
        }

        userAgent.put("os", fullOSName.toString());
    }

    private void setUserAgentProperties(UserAgent data, Map<String, String> userAgent) {
        userAgent.put("name", data.family);

        if (data.major != null) {
            userAgent.put("major", data.major);
        }
        if (data.minor != null) {
            userAgent.put("minor", data.minor);
        }
        if (data.patch != null) {
            userAgent.put("patch", data.patch);
        }
    }

    public static class Factory implements Processor.Factory {
        private final Parser uaParser;

        public Factory() {
            try {
                uaParser = new CachingParser(Resources.getResource("regexes.yaml").openStream());
            } catch (IOException e) {
                throw new RuntimeException("failed to load regexes file from resources", e);
            }
        }

        @Override
        public UserAgentProcessor create(Map<String,Object> config) {
            UserAgentProcessor.Configuration userAgentConfig = JsonUtils.fromJsonMap(UserAgentProcessor.Configuration.class, config);

            return new UserAgentProcessor(userAgentConfig.getField(),
                    userAgentConfig.getTargetField(),
                    uaParser);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;

        public Configuration() { }

        public Configuration(String field, String targetField) {
            this.field = field;
            this.targetField = targetField;
        }

        public String getField() { return field; }

        public String getTargetField() { return targetField; }
    }
}
