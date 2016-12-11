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
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SawmillBenchmarkOptions implements Serializable {

    private JmhOptions jmhOptions;
    private Map<String,Object> pipeline;
    private InputOptions input;
    private ExecutionOptions execution;

    public SawmillBenchmarkOptions() {
    }

    public Options toJmhOptions() {
        jmhOptions.params = new HashMap<String, String>() {{
            put("pipelineConfig", pipeline.toString());
            put("thresholdTimeMs", execution.getThresholdTimeMs());
            put("docsPath", input.getDocsPath());
            put("docType", input.getDocType());
            put("docsAmount", input.getDocsAmount());
            put("docsPerFile", input.getDocsPerFile());
        }};

        return jmhOptions;
    }

    public JmhOptions getJmhOptions() {
        return jmhOptions;
    }

    public Object getPipeline() {
        return pipeline;
    }

    public InputOptions getInput() {
        return input;
    }

    public ExecutionOptions getExecution() {
        return execution;
    }

    public static class JmhOptions implements Options {
        private Integer iterations;
        private String timeout;
        private String runTime;
        private Integer batchSize;
        private Integer warmupIterations;
        private String warmupTime;
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

        private HashMap<String, String> params;

        public JmhOptions() {
        }

        @Override
        public List<String> getIncludes() {
            return regexps;
        }

        @Override
        public List<String> getExcludes() {
            return excludes;
        }

        @Override
        public Optional<String> getOutput() {
            return Optional.eitherOf(output);
        }

        @Override
        public Optional<ResultFormatType> getResultFormat() {
            return Optional.eitherOf(resultFormat);
        }

        @Override
        public Optional<String> getResult() {
            return Optional.eitherOf(result);
        }

        @Override
        public Optional<Boolean> shouldDoGC() {
            return Optional.eitherOf(gcEachIteration);
        }

        @Override
        public List<ProfilerConfig> getProfilers() {
            return profilers;
        }

        @Override
        public Optional<VerboseMode> verbosity() {
            return Optional.eitherOf(verbose);
        }

        @Override
        public Optional<Boolean> shouldFailOnError() {
            return Optional.eitherOf(failOnError);
        }

        @Override
        public Optional<Integer> getThreads() {
            return Optional.eitherOf(threads);
        }

        @Override
        public Optional<int[]> getThreadGroups() {
            if (threadGroups.isEmpty()) {
                return Optional.none();
            } else {
                int[] r = new int[threadGroups.size()];
                for (int c = 0; c < r.length; c++) {
                    r[c] = threadGroups.get(c);
                }
                return Optional.of(r);
            }
        }

        @Override
        public Optional<Boolean> shouldSyncIterations() {
            return Optional.eitherOf(synchIterations);
        }

        @Override
        public Optional<Integer> getWarmupIterations() {
            return Optional.eitherOf(warmupIterations);
        }

        @Override
        public Optional<TimeValue> getWarmupTime() {
            return Optional.eitherOf(warmupTime != null ? TimeValue.valueOf(warmupTime) : null);
        }

        @Override
        public Optional<Integer> getWarmupBatchSize() {
            return Optional.eitherOf(warmupBatchSize);
        }

        @Override
        public Optional<WarmupMode> getWarmupMode() {
            return Optional.eitherOf(warmupMode);
        }

        @Override
        public List<String> getWarmupIncludes() {
            return warmupMicros;
        }

        @Override
        public Optional<Integer> getMeasurementIterations() {
            return Optional.eitherOf(iterations);
        }

        @Override
        public Optional<TimeValue> getMeasurementTime() {
            return Optional.eitherOf(runTime != null ? TimeValue.valueOf(runTime) : null);
        }

        @Override
        public Optional<Integer> getMeasurementBatchSize() {
            return Optional.eitherOf(batchSize);
        }

        @Override
        public Collection<Mode> getBenchModes() {
            return benchMode;
        }

        @Override
        public Optional<TimeUnit> getTimeUnit() {
            return Optional.eitherOf(timeUnit);
        }

        @Override
        public Optional<Integer> getOperationsPerInvocation() {
            return Optional.eitherOf(opsPerInvocation);
        }

        @Override
        public Optional<Integer> getForkCount() {
            return Optional.eitherOf(fork);
        }

        @Override
        public Optional<Integer> getWarmupForkCount() {
            return Optional.eitherOf(warmupFork);
        }

        @Override
        public Optional<String> getJvm() {
            return Optional.eitherOf(jvm);
        }

        @Override
        public Optional<Collection<String>> getJvmArgs() {
            return Optional.eitherOf(jvmArgs);
        }

        @Override
        public Optional<Collection<String>> getJvmArgsAppend() {
            return Optional.eitherOf(jvmArgsAppend);
        }

        @Override
        public Optional<Collection<String>> getJvmArgsPrepend() {
            return Optional.eitherOf(jvmArgsPrepend);
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
            return Optional.eitherOf(timeout != null ? TimeValue.valueOf(timeout) : null);
        }

        public Integer getIterations() {
            return iterations;
        }

        public TimeValue getRunTime() {
            return runTime != null ? TimeValue.valueOf(runTime) : null;
        }

        public Integer getBatchSize() {
            return batchSize;
        }

        public List<Mode> getBenchMode() {
            return benchMode;
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
