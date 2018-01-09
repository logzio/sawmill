package io.logz.sawmill.executor.parser;

import io.logz.sawmill.parser.PipelineDefinition;

public class ExecutionPlanDefinition {
    private final InputDefinition inputDefinition;
    private final PipelineDefinition pipelineDefinition;
    private final OutputDefinition outputDefinition;

    public ExecutionPlanDefinition(InputDefinition inputDefinition, PipelineDefinition pipelineDefinition, OutputDefinition outputDefinition) {
        this.inputDefinition = inputDefinition;
        this.pipelineDefinition = pipelineDefinition;
        this.outputDefinition = outputDefinition;
    }

    public InputDefinition getInputDefinition() {
        return inputDefinition;
    }

    public PipelineDefinition getPipelineDefinition() {
        return pipelineDefinition;
    }

    public OutputDefinition getOutputDefinition() {
        return outputDefinition;
    }
}
