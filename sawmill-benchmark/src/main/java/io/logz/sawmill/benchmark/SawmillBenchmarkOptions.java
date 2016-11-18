package io.logz.sawmill.benchmark;

import org.apache.commons.lang3.StringUtils;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.ProfilerConfig;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.openjdk.jmh.util.Optional;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SawmillBenchmarkOptions implements Options {

    private JmhOptions jmhOptions;
    private String pipeline;
    private InputOptions input;
    private ExecutionOptions execution;

    private HashMap<String, String> params;

    public SawmillBenchmarkOptions() {
    }

    public SawmillBenchmarkOptions toJmhOptions() {
        params = new HashMap<String, String>() {{
            put("pipelineConfig", pipeline);
            put("thresholdTimeMs", execution.getThresholdTimeMs());
            put("docsPath", input.getDocsPath());
            put("docType", input.getDocType());
            put("docsAmount", input.getDocsAmount());
            put("docsPerFile", input.getDocsPerFile());
        }};

        return this;
    }

    @Override
    public List<String> getIncludes() {
        return jmhOptions.getRegexps();
    }

    @Override
    public List<String> getExcludes() {
        return jmhOptions.getExcludes();
    }

    @Override
    public Optional<String> getOutput() {
        return Optional.eitherOf(jmhOptions.getOutput());
    }

    @Override
    public Optional<ResultFormatType> getResultFormat() {
        return Optional.eitherOf(jmhOptions.getResultFormat());
    }

    @Override
    public Optional<String> getResult() {
        return Optional.eitherOf(jmhOptions.getResult());
    }

    @Override
    public Optional<Boolean> shouldDoGC() {
        return Optional.eitherOf(jmhOptions.getGcEachIteration());
    }

    @Override
    public List<ProfilerConfig> getProfilers() {
        return jmhOptions.getProfilers();
    }

    @Override
    public Optional<VerboseMode> verbosity() {
        return Optional.eitherOf(jmhOptions.getVerbose());
    }

    @Override
    public Optional<Boolean> shouldFailOnError() {
        return Optional.eitherOf(jmhOptions.getFailOnError());
    }

    @Override
    public Optional<Integer> getThreads() {
        return Optional.eitherOf(jmhOptions.getThreads());
    }

    @Override
    public Optional<int[]> getThreadGroups() {
        if (jmhOptions.getThreadGroups().isEmpty()) {
            return Optional.none();
        } else {
            int[] r = new int[jmhOptions.getThreadGroups().size()];
            for (int c = 0; c < r.length; c++) {
                r[c] = jmhOptions.getThreadGroups().get(c);
            }
            return Optional.of(r);
        }
    }

    @Override
    public Optional<Boolean> shouldSyncIterations() {
        return Optional.eitherOf(jmhOptions.getSynchIterations());
    }

    @Override
    public Optional<Integer> getWarmupIterations() {
        return Optional.eitherOf(jmhOptions.getWarmupIterations());
    }

    @Override
    public Optional<TimeValue> getWarmupTime() {
        return Optional.eitherOf(jmhOptions.getWarmupTime());
    }

    @Override
    public Optional<Integer> getWarmupBatchSize() {
        return Optional.eitherOf(jmhOptions.getBatchSize());
    }

    @Override
    public Optional<WarmupMode> getWarmupMode() {
        return Optional.eitherOf(jmhOptions.getWarmupMode());
    }

    @Override
    public List<String> getWarmupIncludes() {
        return jmhOptions.getWarmupMicros();
    }

    @Override
    public Optional<Integer> getMeasurementIterations() {
        return Optional.eitherOf(jmhOptions.getIterations());
    }

    @Override
    public Optional<TimeValue> getMeasurementTime() {
        return Optional.eitherOf(jmhOptions.getRunTime());
    }

    @Override
    public Optional<Integer> getMeasurementBatchSize() {
        return Optional.eitherOf(jmhOptions.getBatchSize());
    }

    @Override
    public Collection<Mode> getBenchModes() {
        return jmhOptions.getBenchMode();
    }

    @Override
    public Optional<TimeUnit> getTimeUnit() {
        return Optional.eitherOf(jmhOptions.getTimeUnit());
    }

    @Override
    public Optional<Integer> getOperationsPerInvocation() {
        return Optional.eitherOf(jmhOptions.getOpsPerInvocation());
    }

    @Override
    public Optional<Integer> getForkCount() {
        return Optional.eitherOf(jmhOptions.getFork());
    }

    @Override
    public Optional<Integer> getWarmupForkCount() {
        return Optional.eitherOf(jmhOptions.getWarmupFork());
    }

    @Override
    public Optional<String> getJvm() {
        return Optional.eitherOf(jmhOptions.getJvm());
    }

    @Override
    public Optional<Collection<String>> getJvmArgs() {
        return Optional.eitherOf(jmhOptions.getJvmArgs());
    }

    @Override
    public Optional<Collection<String>> getJvmArgsAppend() {
        return Optional.eitherOf(jmhOptions.getJvmArgsAppend());
    }

    @Override
    public Optional<Collection<String>> getJvmArgsPrepend() {
        return Optional.eitherOf(jmhOptions.getJvmArgsPrepend());
    }

    @Override
    public Optional<Collection<String>> getParameter(String name) {
        String param = params.get(name);
        if (StringUtils.isEmpty(param)){
            return Optional.none();
        } else {
            return Optional.of(Arrays.asList(param));
        }
    }

    @Override
    public Optional<TimeValue> getTimeout() {
        return Optional.eitherOf(jmhOptions.getTimeout());
    }

    public JmhOptions getJmhOptions() {
        return jmhOptions;
    }

    public String getPipeline() {
        return pipeline;
    }

    public InputOptions getInput() {
        return input;
    }

    public ExecutionOptions getExecution() {
        return execution;
    }

    public static class JmhOptions implements Serializable {
        private Integer iterations;
        private TimeValue timeout;
        private TimeValue runTime;
        private Integer batchSize;
        private Integer warmupIterations;
        private TimeValue warmupTime;
        private Integer warmupBatchSize;
        private List<Mode> benchMode = new ArrayList<Mode>();
        private Integer threads;
        private List<Integer> threadGroups = new ArrayList<Integer>();
        private Boolean synchIterations;
        private Boolean gcEachIteration;
        private VerboseMode verbose;
        private Boolean failOnError;
        private List<ProfilerConfig> profilers = new ArrayList<ProfilerConfig>();
        private TimeUnit timeUnit;
        private Integer opsPerInvocation;
        private List<String> regexps = new ArrayList<String>();
        private Integer fork;
        private Integer warmupFork;
        private String output;
        private String result;
        private ResultFormatType resultFormat;
        private String jvm;
        private Collection<String> jvmArgs;
        private Collection<String> jvmArgsAppend;
        private Collection<String> jvmArgsPrepend;
        private List<String> excludes = new ArrayList<String>();
        private WarmupMode warmupMode;
        private List<String> warmupMicros = new ArrayList<String>();

        public JmhOptions() {
        }

        public Integer getIterations() {
            return iterations;
        }

        public TimeValue getTimeout() {
            return timeout;
        }

        public TimeValue getRunTime() {
            return runTime;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public Integer getWarmupIterations() {
            return warmupIterations;
        }

        public TimeValue getWarmupTime() {
            return warmupTime;
        }

        public Integer getWarmupBatchSize() {
            return warmupBatchSize;
        }

        public List<Mode> getBenchMode() {
            return benchMode;
        }

        public Integer getThreads() {
            return threads;
        }

        public List<Integer> getThreadGroups() {
            return threadGroups;
        }

        public Boolean getSynchIterations() {
            return synchIterations;
        }

        public Boolean getGcEachIteration() {
            return gcEachIteration;
        }

        public VerboseMode getVerbose() {
            return verbose;
        }

        public Boolean getFailOnError() {
            return failOnError;
        }

        public List<ProfilerConfig> getProfilers() {
            return profilers;
        }

        public TimeUnit getTimeUnit() {
            return timeUnit;
        }

        public Integer getOpsPerInvocation() {
            return opsPerInvocation;
        }

        public List<String> getRegexps() {
            return regexps;
        }

        public Integer getFork() {
            return fork;
        }

        public Integer getWarmupFork() {
            return warmupFork;
        }

        public String getOutput() {
            return output;
        }

        public String getResult() {
            return result;
        }

        public ResultFormatType getResultFormat() {
            return resultFormat;
        }

        public String getJvm() {
            return jvm;
        }

        public Collection<String> getJvmArgs() {
            return jvmArgs;
        }

        public Collection<String> getJvmArgsAppend() {
            return jvmArgsAppend;
        }

        public Collection<String> getJvmArgsPrepend() {
            return jvmArgsPrepend;
        }

        public List<String> getExcludes() {
            return excludes;
        }

        public WarmupMode getWarmupMode() {
            return warmupMode;
        }

        public List<String> getWarmupMicros() {
            return warmupMicros;
        }
    }

    public static class InputOptions implements Serializable {
        private String docsPath;
        private String docType;
        private String docsAmount;
        private String docsPerFile;

        public InputOptions() {
        }

        public String getDocsPath() {
            return docsPath;
        }

        public String getDocType() {
            return docType;
        }

        public String getDocsAmount() {
            return docsAmount;
        }

        public String getDocsPerFile() {
            return docsPerFile;
        }
    }

    public static class ExecutionOptions implements Serializable {
        private String thresholdTimeMs;

        public ExecutionOptions() {
        }

        public String getThresholdTimeMs() {
            return thresholdTimeMs;
        }
    }
}
