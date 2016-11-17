package io.logz.sawmill.benchmark;

import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.ProfilerConfig;
import org.openjdk.jmh.runner.options.TimeValue;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;
import org.openjdk.jmh.util.Optional;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class SawmillBenchmarkOptions implements Options {
    // JMH Config
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

    //---------------------------------------------------------------
    // Sawmill config
    private List<String> pipelineConfig;
    private List<String> docsPath;
    private List<String> docType;
    private List<String> thresholdTimeMs;
    private List<String> docsAmount;
    private List<String> docsPerFile;

    private HashMap<String, List<String>> params;

    public SawmillBenchmarkOptions() {
    }

    public SawmillBenchmarkOptions withParams() {
        params = new HashMap<String, List<String>>() {{
            put("pipelineConfig", pipelineConfig);
            put("docsPath", docsPath);
            put("docType", docType);
            put("thresholdTimeMs", thresholdTimeMs);
            put("docsAmount", docsAmount);
            put("docsPerFile", docsPerFile);
        }};

        return this;
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
        return Optional.eitherOf(warmupTime);
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
        return Optional.eitherOf(runTime);
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
        Collection<String> list = params.get(name);
        if (list == null || list.isEmpty()){
            return Optional.none();
        } else {
            return Optional.of(list);
        }
    }

    @Override
    public Optional<TimeValue> getTimeout() {
        return Optional.eitherOf(timeout);
    }

    // Getters for Jackson parsing
    public Integer getIterations() {
        return iterations;
    }

    public TimeValue getRunTime() {
        return runTime;
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

    public List<String> getPipelineConfig() {
        return pipelineConfig;
    }

    public List<String> getDocsPath() {
        return docsPath;
    }

    public List<String> getDocType() {
        return docType;
    }

    public List<String> getThresholdTimeMs() {
        return thresholdTimeMs;
    }

    public List<String> getDocsAmount() {
        return docsAmount;
    }

    public List<String> getDocsPerFile() {
        return docsPerFile;
    }
}
