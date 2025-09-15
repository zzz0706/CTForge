package org.apache.hadoop.security;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestKDiagKinitCommandReport {

    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private final PrintStream originalOut = System.out;

    @Before
    public void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalOut);
    }

    @Test
    public void testKDiagReportsKinitCommandInExecuteSummary() throws Exception {
        // 1. Instantiate Configuration (defaults or test-resource overrides are used)
        Configuration conf = new Configuration();

        // 2. Read the expected value dynamically
        String expectedKinit = conf.get(
            "hadoop.kerberos.kinit.command",
            "kinit"
        );

        // 3. Prepare the test conditions: disable Kerberos login check
        conf.set("hadoop.security.authentication", "simple");

        // 4. Instantiate KDiag and run execute() with --nologin flag to skip login check
        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        // In Hadoop 2.8.5 KDiag.execute() takes no arguments; use ToolRunner instead
        org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});

        // 5. Verify the diagnostic output contains the expected line
        String output = outContent.toString();
        String expectedLine = "hadoop.kerberos.kinit.command = " + expectedKinit;
        assertTrue("Diagnostic output should contain '" + expectedLine + "'",
                   Pattern.compile(Pattern.quote(expectedLine)).matcher(output).find());
    }
}