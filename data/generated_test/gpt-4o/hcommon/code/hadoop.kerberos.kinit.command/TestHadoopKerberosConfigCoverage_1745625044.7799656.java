package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.*;

public class TestHadoopKerberosConfigCoverage {
    // Get configuration value using API
    // Prepare the input conditions for testing the `validateKinitExecutable` method in KDiag with a valid kinit configuration.
    @Test
    public void testValidateKinitExecutable_withValidConfiguration() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Setting a valid kinit command path in the configuration
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Create an instance of KDiag with the configuration
        KDiag kdiag = new KDiag(conf);

        try {
            // Call the validateKinitExecutable method
            kdiag.validateKinitExecutable();
        } catch (IOException e) {
            fail("Unexpected exception during kinit validation for valid configuration: " + e.getMessage());
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for testing the `validateKinitExecutable` method in KDiag with an invalid kinit configuration.
    @Test
    public void testValidateKinitExecutable_withInvalidConfiguration() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set an invalid kinit command path in the configuration
        conf.set("hadoop.kerberos.kinit.command", "/invalid/path/kinit");

        // Create an instance of KDiag with the configuration
        KDiag kdiag = new KDiag(conf);

        try {
            // Call the validateKinitExecutable method
            kdiag.validateKinitExecutable();
            fail("Expected IOException due to invalid kinit configuration");
        } catch (IOException e) {
            // Verify the error message includes information about the invalid kinit path
            assertTrue(e.getMessage().contains("/invalid/path/kinit"));
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for testing the `execute` method in KDiag with a valid configuration.
    @Test
    public void testKDiagExecute_withValidConfiguration() throws Exception {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Configure Kerberos authentication and a valid kinit path
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Create an instance of KDiag with the configuration
        KDiag kdiag = new KDiag(conf);

        // Execute the diagnostics
        boolean diagnosticsSuccess = kdiag.execute();

        // Ensure the diagnostics were successful
        assertTrue(diagnosticsSuccess);
    }

    // Get configuration value using API
    // Prepare the input conditions for testing the `loginUserFromSubject` method with an invalid subject.
    @Test
    public void testLoginUserFromSubject_withInvalidSubject() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Set the Kerberos authentication method in the configuration
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply the configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Attempt to log in using a null subject
            UserGroupInformation.loginUserFromSubject(null);
            fail("Expected IOException due to null Subject provided to loginUserFromSubject");
        } catch (IOException e) {
            // Verify the exception contains details about the failure
            assertTrue(e.getMessage().contains("failure to login"));
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for testing spawnAutoRenewalThreadForUserCreds.
    @Test
    public void testSpawnAutoRenewalThread_forKerberosAuthentication() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Configure Kerberos authentication in the configuration
        conf.set("hadoop.security.authentication", "kerberos");

        // Apply the configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        try {
            // Simulate a Kerberos user login using a valid, empty subject
            Subject subject = new Subject();
            UserGroupInformation.loginUserFromSubject(subject);

            // Assert that the current user belongs to kerberos
            assertTrue(UserGroupInformation.getCurrentUser().toString().contains("kerberos"));
        } catch (IOException e) {
            fail("Unexpected exception during login or credential renewal: " + e.getMessage());
        }
    }
}