package io.logz.sawmill.executor.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.parser.PipelineDefinition;
import io.logz.sawmill.parser.PipelineDefinitionJsonParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

public class ExecutionPlanDefinitionJsonParser {
    private final InputJsonParser inputJsonParser;
    private final PipelineDefinitionJsonParser pipelineDefinitionJsonParser;
    private final OutputJsonParser outputJsonParser;

    public ExecutionPlanDefinitionJsonParser() {
        this.inputJsonParser = new InputJsonParser();
        this.pipelineDefinitionJsonParser = new PipelineDefinitionJsonParser();
        this.outputJsonParser = new OutputJsonParser();
    }

    public ExecutionPlanDefinition parse(String config) {
        String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
        Map<String, Object> configMap = JsonUtils.fromJsonString(new TypeReference<Map<String, Object>>() {}, configJson);
        return parse(configMap);
    }

    private ExecutionPlanDefinition parse(Map<String, Object> configMap) {
        InputDefinition inputDefinition = inputJsonParser.parse(configMap);
        PipelineDefinition pipelineDefinition = pipelineDefinitionJsonParser.parse(configMap);
        OutputDefinition outputDefinition = outputJsonParser.parse(configMap);

        return new ExecutionPlanDefinition(inputDefinition, pipelineDefinition, outputDefinition);
    }
}
