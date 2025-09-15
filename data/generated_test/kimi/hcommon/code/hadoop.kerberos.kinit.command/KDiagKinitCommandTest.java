package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;

import static org.junit.Assert.assertTrue;

public class KDiagKinitCommandTest {

    private File tempKinit;
    private ByteArrayOutputStream diagOut;
    private PrintStream originalOut;

    @Before
    public void setUp() throws Exception {
        // create a temporary kinit file
        tempKinit = File.createTempFile("kinit", ".sh");
        tempKinit.setExecutable(true);
        Files.write(tempKinit.toPath(), "#!/bin/bash".getBytes());
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
    }

    @Test
    public void testKDiagPrintsAbsoluteKinitPathWhenConfigured() throws Exception {
        Configuration conf = new Configuration();
        String absolutePath = tempKinit.getAbsolutePath();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, absolutePath);

        // KDiag.execute() takes no arguments in 2.8.5; use main() instead
        String[] args = {"-conf", absolutePath};
        KDiag.main(args);

        String output = diagOut.toString();
        assertTrue(output.contains(KDiag.KERBEROS_KINIT_COMMAND + " = " + absolutePath));
    }
}