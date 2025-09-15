package org.apache.hadoop.security;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
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
        Configuration conf = new Configuration();
        String expectedKinit = conf.get("hadoop.kerberos.kinit.command", "kinit");
        conf.set("hadoop.security.authentication", "simple");

        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        try {
            org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});
        } catch (Exception e) {
            // ignore expected KDiag failures
        }

        String output = outContent.toString();
        String expectedLine = "hadoop.kerberos.kinit.command = " + expectedKinit;
        assertTrue("Diagnostic output should contain '" + expectedLine + "'",
                   Pattern.compile(Pattern.quote(expectedLine)).matcher(output).find());
    }

    @Test
    public void testKDiagWithAbsoluteKinitPath() throws Exception {
        Configuration conf = new Configuration();
        File fakeKinit = File.createTempFile("kinit", null);
        fakeKinit.delete();
        fakeKinit = new File(fakeKinit.getAbsolutePath() + ".sh");
        fakeKinit.createNewFile();
        fakeKinit.setExecutable(true);
        fakeKinit.deleteOnExit();

        conf.set("hadoop.kerberos.kinit.command", fakeKinit.getAbsolutePath());
        conf.set("hadoop.security.authentication", "simple");

        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        try {
            org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});
        } catch (Exception e) {
            // ignore expected KDiag failures
        }

        String output = outContent.toString();
        assertTrue("Output should mention absolute path",
                   output.contains(fakeKinit.getAbsolutePath()));
    }

    @Test
    public void testKDiagWithInvalidAbsoluteKinitPath() throws Exception {
        Configuration conf = new Configuration();
        String invalidPath = "/nonexistent/path/to/kinit";
        conf.set("hadoop.kerberos.kinit.command", invalidPath);
        conf.set("hadoop.security.authentication", "simple");

        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        try {
            org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});
        } catch (Exception e) {
            // ignore expected KDiag failures
        }

        String output = outContent.toString();
        assertTrue("Output should contain error for invalid kinit path",
                   output.contains("not found") || output.contains("does not exist"));
    }

    @Test
    public void testKDiagWithRelativeKinitCommand() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "custom-kinit");
        conf.set("hadoop.security.authentication", "simple");

        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        try {
            org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});
        } catch (Exception e) {
            // ignore expected KDiag failures
        }

        String output = outContent.toString();
        assertTrue("Output should mention relative command and PATH",
                   output.contains("relative") || output.contains("PATH"));
    }

    @Test
    public void testKDiagWithEmptyKinitCommand() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "");
        conf.set("hadoop.security.authentication", "simple");

        KDiag kdiag = new KDiag();
        kdiag.setConf(conf);
        try {
            org.apache.hadoop.util.ToolRunner.run(conf, kdiag, new String[]{"--nologin"});
        } catch (Exception e) {
            // ignore expected KDiag failures
        }

        String output = outContent.toString();
        assertTrue("Output should contain kinit command even when empty",
                   output.contains("hadoop.kerberos.kinit.command ="));
    }
}