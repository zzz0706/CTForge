package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KDiag;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestHadoopCommonConfigurationUsage {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testLoginUserFromSubject_withInvalidSubject() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set the Kerberos authentication method
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply the configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Call loginUserFromSubject with a null subject
            UserGroupInformation.loginUserFromSubject(null);
            fail("Expected IOException due to null Subject");
        } catch (IOException e) {
            // Verify exception message contains relevant error
            assertTrue(e.getMessage().contains("failure to login"));
        }
    }

    @Test
    public void testValidateKinitExecutable_withValidPath() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set a valid KERBEROS_KINIT_COMMAND path
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Create an instance of KDiag
        KDiag kdiag = new KDiag(conf);

        try {
            // Test the validateKinitExecutable method
            kdiag.validateKinitExecutable();
        } catch (Exception e) {
            fail("Unexpected exception during validateKinitExecutable: " + e.getMessage());
        }
    }

    @Test
    public void testValidateKinitExecutable_withInvalidPath() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set an invalid KERBEROS_KINIT_COMMAND path
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/invalid/path/kinit");

        // Create an instance of KDiag
        KDiag kdiag = new KDiag(conf);

        try {
            // Test the validateKinitExecutable method
            kdiag.validateKinitExecutable();
            fail("Expected exception due to invalid kinit path");
        } catch (IOException e) {
            // Verify exception message contains error for invalid path
            assertTrue(e.getMessage().contains("Invalid kinit path"));
        }
    }

    @Test
    public void testExecute_withValidConfiguration() throws Exception {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Simulate Kerberos enabled and set valid properties
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Create an instance of KDiag
        KDiag kdiag = new KDiag(conf);

        // Test the execute method
        boolean result = kdiag.execute();

        // Verify success result indicates diagnostics passed
        assertTrue(result);
    }

    @Test
    public void testSpawnAutoRenewalThread_forKerberosUser() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set Kerberos authentication method
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Simulate user login with Subject
            Subject subject = new Subject();
            UserGroupInformation.loginUserFromSubject(subject);

            // Verify Kerberos credential renewal thread spawned correctly
            assertTrue(UserGroupInformation.getCurrentUser().toString().contains("kerberos"));
        } catch (IOException e) {
            fail("Unexpected exception while spawning auto renewal thread: " + e.getMessage());
        }
    }
}