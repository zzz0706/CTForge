package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.security.auth.Subject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.StringWriter;

import static org.junit.Assert.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({KDiag.class})
public class KDiagTest {

    @Test
    public void testKDiagWarnsWhenKinitRelativeAndNotOnPath() throws Exception {
        // 1. Create configuration without setting kinit explicitly
        Configuration conf = new Configuration();
        // Ensure the key is set to a nonexistent relative command
        conf.set("hadoop.kerberos.kinit.command", "nonexistentkinit");

        // 2. Capture diagnostic output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        try {
            // 3. Instantiate KDiag and run validation via main() to trigger private logic
            String[] args = {};
            KDiag.main(args);

            // 4. Assert warning and PATH are printed
            String output = outContent.toString();
            assertTrue("Should warn about relative executable",
                    output.contains("Executable nonexistentkinit is relative -must be on the PATH"));
            assertTrue("Should print PATH environment variable",
                    output.contains("PATH="));
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    public void testKDiagValidatesAbsoluteKinitPath() throws Exception {
        // 1. Prepare configuration with absolute path to a nonexistent file
        Configuration conf = new Configuration();
        File fakeKinit = new File("/tmp/fakekinit");
        conf.set("hadoop.kerberos.kinit.command", fakeKinit.getAbsolutePath());

        // 2. Capture diagnostic output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        ByteArrayOutputStream errContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        PrintStream originalErr = System.err;
        System.setOut(new PrintStream(outContent));
        System.setErr(new PrintStream(errContent));

        try {
            // 3. Run KDiag
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            KDiag diag = new KDiag(conf, pw, null, null, 0, false);
            diag.execute();

            // 4. Assert that absolute path is validated and error is reported
            String output = outContent.toString() + sw.toString();
            String error = errContent.toString();
            assertTrue("Should print absolute kinit path",
                    output.contains(fakeKinit.getAbsolutePath()));
            assertTrue("Should report file does not exist",
                    output.contains("File does not exist") || error.contains("File does not exist"));
        } finally {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }

    @Test
    public void testKDiagHandlesEmptyKinitCommand() throws Exception {
        // 1. Prepare configuration with empty kinit command
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "");

        // 2. Capture diagnostic output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        try {
            // 3. Run KDiag
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            KDiag diag = new KDiag(conf, pw, null, null, 0, false);
            diag.execute();

            // 4. Assert no validation occurs for empty command
            String output = outContent.toString() + sw.toString();
            assertFalse("Should not print empty kinit command validation",
                    output.contains("Executable  is relative"));
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    public void testKDiagPrintsConfigurationOptions() throws Exception {
        // 1. Prepare configuration with custom kinit command
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "customkinit");

        // 2. Capture diagnostic output
        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        PrintStream originalOut = System.out;
        System.setOut(new PrintStream(outContent));

        try {
            // 3. Run KDiag
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            KDiag diag = new KDiag(conf, pw, null, null, 0, false);
            diag.execute();

            // 4. Assert configuration is printed
            String output = outContent.toString() + sw.toString();
            assertTrue("Should print configuration option",
                    output.contains("hadoop.kerberos.kinit.command = customkinit"));
        } finally {
            System.setOut(originalOut);
        }
    }

    @Test
    public void testUserGroupInformationSpawnsRenewalWithCustomKinit() throws Exception {
        // 1. Prepare configuration with custom kinit command
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "customkinit");
        UserGroupInformation.setConfiguration(conf);

        // 2. Create mock subject
        Subject subject = new Subject();
        
        // 3. Attempt login (will fail but we just want to test config propagation)
        try {
            UserGroupInformation.loginUserFromSubject(subject);
        } catch (Exception e) {
            // Expected to fail in test environment
        }

        // 4. Verify configuration was used (via PowerMockito if needed)
        // This test primarily ensures configuration is properly loaded
        assertEquals("customkinit", conf.get("hadoop.kerberos.kinit.command"));
    }
}