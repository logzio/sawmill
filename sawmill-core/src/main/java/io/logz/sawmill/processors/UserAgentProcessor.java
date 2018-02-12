package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import io.logz.sawmill.utilities.UserAgentParserProvider;
import org.apache.commons.lang3.StringUtils;
import ua_parser.Client;
import ua_parser.OS;
import ua_parser.UserAgent;

import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "userAgent", factory = UserAgentProcessor.Factory.class)
public class UserAgentProcessor implements Processor {
    private final String field;
    private final Template targetField;
    private final String prefix;
    private final int truncatedInputLength;
    private final List<String> tagOnTruncated;
    private final UserAgentParserProvider uaParserProvider;

    public UserAgentProcessor(String field, Template targetField, String prefix, int truncatedInputLength, List<String> tagOnTruncated, UserAgentParserProvider userAgentParserProvider) {
        this.field = requireNonNull(field, "field cannot be null");
        this.targetField = targetField;
        this.prefix = prefix != null ? prefix : "";
        this.uaParserProvider = requireNonNull(userAgentParserProvider);
        this.truncatedInputLength = truncatedInputLength > 0 ? truncatedInputLength : 256;
        this.tagOnTruncated = tagOnTruncated;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field, String.class)) {
            return ProcessResult.failure(String.format("failed to parse user agent, couldn't find field [%s] or not instance of [%s]", field, String.class));
        }

        String uaString = doc.getField(field);

        if (uaString.length() > truncatedInputLength) {
            uaString = uaString.substring(0, truncatedInputLength);
            doc.appendList("tags", tagOnTruncated);
        }

        Client client = uaParserProvider.provide().parse(uaString);

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
            doc.addField(targetField.render(doc), userAgent);
        } else {
            userAgent.entrySet().forEach(property -> {
                doc.addField(prefix + property.getKey(), property.getValue());
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
        private final UserAgentParserProvider uaParserProvider;
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
            uaParserProvider = new UserAgentParserProvider();
        }

        @Override
        public UserAgentProcessor create(Map<String,Object> config) {
            UserAgentProcessor.Configuration userAgentConfig = JsonUtils.fromJsonMap(UserAgentProcessor.Configuration.class, config);

            Template targetField = StringUtils.isEmpty(userAgentConfig.getTargetField()) ? null : templateService.createTemplate(userAgentConfig.getTargetField());
            return new UserAgentProcessor(userAgentConfig.getField(),
                    targetField,
                    userAgentConfig.getPrefix(),
                    userAgentConfig.getTruncatedInputLength(),
                    userAgentConfig.getTagOnTruncated(),
                    uaParserProvider);
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;
        private String prefix;
        private int truncatedInputLength = 256;
        private List<String> tagOnTruncated = Collections.singletonList("_user_agent_truncated");

        public Configuration() { }

        public Configuration(String field, String targetField) {
            this.field = field;
            this.targetField = targetField;
        }

        public String getField() { return field; }

        public String getTargetField() { return targetField; }

        public String getPrefix() {
            return prefix;
        }

        public int getTruncatedInputLength() {
            return truncatedInputLength;
        }

        public List<String> getTagOnTruncated() {
            return tagOnTruncated;
        }
    }
}
