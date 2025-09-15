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

    // Test the loginUserFromSubject method with an invalid subject.
    @Test
    public void testLoginUserFromSubject_withInvalidSubject() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set the Kerberos authentication method in the configuration
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply the configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Invoke loginUserFromSubject with a null subject
            UserGroupInformation.loginUserFromSubject(null);
            fail("Expected IOException due to null Subject");
        } catch (IOException e) {
            // Assert that the exception message contains relevant error information
            assertTrue(e.getMessage().contains("failure to login"));
        }
    }

    // Test the validateKinitExecutable method with a valid kinit path
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

    // Test the validateKinitExecutable method with an invalid kinit path
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
            // Assert that the exception message contains error related to the invalid path
            assertTrue(e.getMessage().contains("Invalid kinit path"));
        }
    }

    // Test the execute method in KDiag with valid Kerberos configuration
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

        // Verify the result indicates diagnostic success
        assertTrue(result);
    }

    // Test the spawnAutoRenewalThreadForUserCreds method in UserGroupInformation
    @Test
    public void testSpawnAutoRenewalThread_forKerberosUser() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set Kerberos authentication method
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Simulate subject login for a Kerberos user
            Subject subject = new Subject();
            UserGroupInformation.loginUserFromSubject(subject);

            // Assert that the Kerberos credential renewal thread has been spawned
            assertTrue(UserGroupInformation.getCurrentUser().toString().contains("kerberos"));
        } catch (IOException e) {
            fail("Unexpected exception while spawning auto-renewal thread: " + e.getMessage());
        }
    }
}