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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;

import java.io.File;
import java.io.IOException;

/**
 * Created by roiravhon on 4/6/17.
 */
public class BenchmarkTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Test
    public void testApache() throws IOException, RunnerException {
        String scenario = Resources.toString(Resources.getResource("ApacheScenario.conf"), Charsets.UTF_8);
        scenario = scenario.replaceAll("DOCUMENTPLACEHOLDER", tempFolder.getRoot().getAbsolutePath());

        String json = ConfigFactory.parseString(scenario).root().render(ConfigRenderOptions.concise());
        Options opts = JsonUtils.fromJsonString(SawmillBenchmarkOptions.class, json).toJmhOptions();
        new Runner(opts).run();

        // Copy the artifacts
        File sourceResults = new File(tempFolder.getRoot(), "result.json");
        Files.copy(sourceResults, new File(targetDir(), "result.json"));

        System.out.println("\n\nTest Output:");
        System.out.println(FileUtils.readFileToString(sourceResults, Charsets.UTF_8));
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
