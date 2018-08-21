package io.logz.sawmill.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class PipelineDefinitionJsonParser {

    public PipelineDefinition parse(String config) {
        String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
        Map<String, Object> configMap = JsonUtils.fromJsonString(new TypeReference<Map<String, Object>>() {}, configJson);
        return parse(configMap);
    }

    private PipelineDefinition parse(Map<String, Object> configMap) {
        List<Map<String, Object>> executionSteps = JsonUtils.getList(configMap, "steps", true);
        List<ExecutionStepDefinition> executionStepDefinitionList = ExecutionStepDefinitionParser.parse(executionSteps);

        Boolean stopOnFailure = JsonUtils.getBoolean(this, configMap, "stopOnFailure", false);

        return new PipelineDefinition(executionStepDefinitionList, stopOnFailure);
    }

}
