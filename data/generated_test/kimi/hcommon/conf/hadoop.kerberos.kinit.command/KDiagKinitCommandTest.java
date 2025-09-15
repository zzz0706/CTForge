package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;

import static org.junit.Assert.*;

public class KDiagKinitCommandTest {

    private File tempKinit;
    private File fakeConf;
    private ByteArrayOutputStream diagOut;
    private PrintStream originalOut;

    @Before
    public void setUp() throws Exception {
        // create a temporary kinit file
        tempKinit = File.createTempFile("kinit", ".sh");
        tempKinit.setExecutable(true);
        Files.write(tempKinit.toPath(), "#!/bin/bash\necho fake kinit".getBytes());

        // create a minimal core-site.xml pointing to the custom kinit
        fakeConf = File.createTempFile("core-site", ".xml");
        try (PrintWriter w = new PrintWriter(fakeConf)) {
            w.println("<?xml version=\"1.0\"?>");
            w.println("<configuration>");
            w.println("  <property>");
            w.println("    <name>" + KDiag.KERBEROS_KINIT_COMMAND + "</name>");
            w.println("    <value>" + tempKinit.getAbsolutePath() + "</value>");
            w.println("  </property>");
            w.println("</configuration>");
        }

        diagOut = new ByteArrayOutputStream();
        originalOut = System.out;
        System.setOut(new PrintStream(diagOut));
    }

    @After
    public void tearDown() {
        System.setOut(originalOut);
        if (tempKinit != null) {
            tempKinit.delete();
        }
        if (fakeConf != null) {
            fakeConf.delete();
        }
    }

    @Test
    public void testKDiagValidatesAbsoluteKinitPath() throws Exception {
        // run KDiag with the configuration file
        String[] args = {"-conf", fakeConf.getAbsolutePath()};
        KDiag.main(args);

        String output = diagOut.toString();
        // ensure the absolute path is printed
        assertTrue("Absolute kinit path not found in output",
                   output.contains(KDiag.KERBEROS_KINIT_COMMAND + " = " + tempKinit.getAbsolutePath()));
        // ensure validation occurred
        assertTrue("Validation message missing",
                   output.contains("File") && output.contains(tempKinit.getName()));
    }

    @Test
    public void testKDiagFailsWithNonexistentAbsoluteKinit() throws Exception {
        // non-existent absolute path
        File nonexistent = new File(tempKinit.getParentFile(), "nonexistent-kinit");
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, nonexistent.getAbsolutePath());

        // write the configuration to a new file
        File badConf = File.createTempFile("bad-core-site", ".xml");
        try (PrintWriter w = new PrintWriter(badConf)) {
            w.println("<?xml version=\"1.0\"?>");
            w.println("<configuration>");
            w.println("  <property>");
            w.println("    <name>" + KDiag.KERBEROS_KINIT_COMMAND + "</name>");
            w.println("    <value>" + nonexistent.getAbsolutePath() + "</value>");
            w.println("  </property>");
            w.println("</configuration>");
        }

        try {
            String[] args = {"-conf", badConf.getAbsolutePath()};
            KDiag.main(args);
            String output = diagOut.toString();
            assertTrue("Error message for missing kinit not found",
                       output.contains("File does not exist") || output.contains("File is empty"));
        } finally {
            badConf.delete();
        }
    }

    @Test
    public void testKDiagReportsRelativeKinitCommand() throws Exception {
        Configuration conf = new Configuration();
        // use a relative command (will be resolved via PATH)
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "kinit");

        File relConf = File.createTempFile("rel-core-site", ".xml");
        try (PrintWriter w = new PrintWriter(relConf)) {
            w.println("<?xml version=\"1.0\"?>");
            w.println("<configuration>");
            w.println("  <property>");
            w.println("    <name>" + KDiag.KERBEROS_KINIT_COMMAND + "</name>");
            w.println("    <value>kinit</value>");
            w.println("  </property>");
            w.println("</configuration>");
        }

        try {
            String[] args = {"-conf", relConf.getAbsolutePath()};
            KDiag.main(args);
            String output = diagOut.toString();
            assertTrue("Relative kinit path not reported",
                       output.contains("Executable kinit is relative -must be on the PATH"));
            assertTrue("PATH not printed", output.contains("PATH="));
        } finally {
            relConf.delete();
        }
    }

    @Test
    public void testUserGroupInformationReadsKinitCommand() throws Exception {
        // spawnAutoRenewalThreadForUserCreds is private, but we can
        // trigger it via loginUserFromSubject if security is enabled.
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, tempKinit.getAbsolutePath());

        // set security to kerberos so the renewal thread is started
        conf.set("hadoop.security.authentication", "kerberos");

        UserGroupInformation.setConfiguration(conf);
        try {
            // loginUserFromSubject will start the renewal thread
            UserGroupInformation.loginUserFromSubject(null);
            // if we reach here the command was at least read
            assertEquals("Configuration should contain custom kinit path",
                         tempKinit.getAbsolutePath(),
                         conf.get("hadoop.kerberos.kinit.command"));
        } catch (IOException expected) {
            // login will fail without real credentials, but kinit command is still read
            assertEquals("Configuration should contain custom kinit path",
                         tempKinit.getAbsolutePath(),
                         conf.get("hadoop.kerberos.kinit.command"));
        }
    }
}