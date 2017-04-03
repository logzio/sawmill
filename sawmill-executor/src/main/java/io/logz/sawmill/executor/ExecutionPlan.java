package io.logz.sawmill.executor;

import io.logz.sawmill.Pipeline;

public class ExecutionPlan {
    private final Input input;
    private final Pipeline pipeline;
    private final Output output;

    public ExecutionPlan(Input input, Pipeline pipeline, Output output) {
        this.input = input;
        this.pipeline = pipeline;
        this.output = output;
    }

    public static final class Factory {
        private final Pipeline.Factory pipelineFactory;

        public Factory() {
            this(new InputFactoryRegistry(), new OutputFactoryRegistry());
        }

        public Factory(InputFactoryRegistry inputFactoryRegistry, OutputFactoryRegistry outputFactoryRegistry) {
            InputFactoriesLoader.getInstance().loadAnnotatedInputs(inputFactoryRegistry);
            OutputFactoriesLoader.getInstance().loadAnnotatedOutputs(outputFactoryRegistry);
            pipelineFactory = new Pipeline.Factory();
        }

        public ExecutionPlan create(String config) {
            return null;
        }
    }
}
