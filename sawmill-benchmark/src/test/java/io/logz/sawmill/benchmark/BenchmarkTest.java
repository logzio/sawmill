package io.logz.sawmill.benchmark;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

public class BenchmarkTest {

    private static final Logger logger = LoggerFactory.getLogger(BenchmarkTest.class);

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testApache() throws IOException, RunnerException {
        testScenario("ApacheScenario.conf", 17_500.0);
    }

    @Test
    public void testTemplate() throws IOException, RunnerException {
        testScenario("TemplateScenario.conf", 17_500.0);
    }

    private void testScenario(String scenarioName, double threshold) throws RunnerException, IOException {
        String scenario = Resources.toString(Resources.getResource(scenarioName), Charsets.UTF_8);
        scenario = scenario.replaceAll("DOCUMENTPLACEHOLDER", tempFolder.getRoot().getAbsolutePath());

        String json = ConfigFactory.parseString(scenario).root().render(ConfigRenderOptions.concise());
        Options opts = JsonUtils.fromJsonString(SawmillBenchmarkOptions.class, json).toJmhOptions();
        Iterator<RunResult> results = new Runner(opts).run().iterator();

        // Copy the artifacts
        File sourceResults = new File(tempFolder.getRoot(), "result.json");
        Files.copy(sourceResults, new File(targetDir(), "result.json"));

        logger.info("Test Output:");
        logger.info(FileUtils.readFileToString(sourceResults, Charsets.UTF_8));

        while ( results.hasNext()) {
            RunResult runResults = results.next();
            assertThat(runResults.getPrimaryResult().getScore()).isGreaterThan(threshold);
        }
    }

    // Not the cleanest way to get the target dir to save the result
    private File targetDir(){
        String relPath = getClass().getProtectionDomain().getCodeSource().getLocation().getFile();
        File targetDir = new File(relPath + "../../target");
        if(!targetDir.exists()) {
            targetDir.mkdir();
        }
        return targetDir;
    }
}
