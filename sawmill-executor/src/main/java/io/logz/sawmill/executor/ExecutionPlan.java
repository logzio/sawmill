package io.logz.sawmill.executor;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ExecutionResult;
import io.logz.sawmill.Pipeline;
import io.logz.sawmill.PipelineExecutor;
import io.logz.sawmill.executor.parser.ExecutionPlanDefinition;
import io.logz.sawmill.executor.parser.ExecutionPlanDefinitionJsonParser;
import io.logz.sawmill.executor.parser.InputParser;
import io.logz.sawmill.executor.parser.OutputParser;

import java.util.UUID;

public class ExecutionPlan {
    private final Input input;
    private final Pipeline pipeline;
    private final Output output;

    public ExecutionPlan(Input input, Pipeline pipeline, Output output) {
        this.input = input;
        this.pipeline = pipeline;
        this.output = output;
    }

    public Input getInput() {
        return input;
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public Output getOutput() {
        return output;
    }

    public static final class Factory {
        private final Pipeline.Factory pipelineFactory;
        private final ExecutionPlanDefinitionJsonParser executionPlanDefinitionJsonParser;
        private final InputParser inputParser;
        private final OutputParser outputParser;

        public Factory() {
            this(new InputFactoryRegistry(), new OutputFactoryRegistry());
        }

        public Factory(InputFactoryRegistry inputFactoryRegistry, OutputFactoryRegistry outputFactoryRegistry) {
            InputFactoriesLoader.getInstance().loadAnnotatedInputs(inputFactoryRegistry);
            OutputFactoriesLoader.getInstance().loadAnnotatedOutputs(outputFactoryRegistry);
            pipelineFactory = new Pipeline.Factory();
            executionPlanDefinitionJsonParser = new ExecutionPlanDefinitionJsonParser();
            inputParser = new InputParser(inputFactoryRegistry);
            outputParser = new OutputParser(outputFactoryRegistry);
        }

        public ExecutionPlan create(String config) {
            String id = UUID.randomUUID().toString();
            ExecutionPlanDefinition executionPlanDefinition = executionPlanDefinitionJsonParser.parse(config);
            Input input = inputParser.parse(executionPlanDefinition.getInputDefinition());
            Pipeline pipeline = pipelineFactory.create(id, executionPlanDefinition.getPipelineDefinition());
            Output output = outputParser.parse(executionPlanDefinition.getOutputDefinition());

            return new ExecutionPlan(input, pipeline, output);
        }
    }
}
