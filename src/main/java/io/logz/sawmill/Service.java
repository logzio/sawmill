package io.logz.sawmill;

import io.logz.sawmill.processors.SampleProcessor;

import java.util.HashMap;
import java.util.Map;

public class Service {
    private final Map<String, Processor.Factory> processorFactories;
    private final Pipeline.Factory pipelineFactory = new Pipeline.Factory();
    private final PipelineExecutor pipelineExecutor;

    public Service() {
        this.pipelineExecutor = new PipelineExecutor();
        this.processorFactories = new HashMap<>();

        loadBaseProcessors();
        loadPluginProcessors();
    }

    private void loadPluginProcessors() {
    }

    private void loadBaseProcessors() {
        processorFactories.put(SampleProcessor.TYPE, new SampleProcessor.Factory());
    }

    public PipelineExecutor getPipelineExecutor() {
        return pipelineExecutor;
    }

    public Pipeline createPipeline(Map<String, Object> config) {
        return pipelineFactory.create(config, processorFactories);
    }
}
