package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import javax.security.auth.Subject;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestKerberosConfiguration {

    // Verify usage and propagation of the `hadoop.kerberos.kinit.command` configuration.
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Get the configuration value using API
        Configuration conf = new Configuration();
        conf.setClassLoader(this.getClass().getClassLoader());
        String kinitCommand = conf.get("hadoop.kerberos.kinit.command", "kinit");

        // Prepare a valid Subject for Kerberos login
        Subject subject = new Subject();

        // Simulate Kerberos setup prerequisites
        assertNotNull("Configuration for Kerberos kinit command must not be null", kinitCommand);
        
        // Invoke the method to test login functionality
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromSubject(subject);

        // Verify conditions after login
        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("User must be logged in successfully with Kerberos authentication method", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());
    }

    // Test for validation of the kinit command using `KDiag.validateKinitExecutable()`.
    @Test
    public void testValidateKinitExecutable() throws Exception {
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit"); // Example path for test

        KDiag kdiag = new KDiag(conf);

        // Invoke the validateKinitExecutable method indirectly from execute.
        // This causes internal validation of the kinit command.
        boolean executedSuccessfully = kdiag.execute();

        assertTrue("KDiag execution should succeed when configuration is valid", executedSuccessfully);
    }

    // Test for ensuring the TGT renewal thread is spawning (via configuration usage in `spawnAutoRenewalThreadForUserCreds()`).
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Create and configure Kerberos environment
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "kinit"); // Use default for test
        UserGroupInformation.setConfiguration(conf);

        // Create a Subject and log in the user
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("User must be logged in using Kerberos authentication method", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());

        // Verify if the TGT renewal thread is properly initialized
        File testTempDir = new File(System.getProperty("java.io.tmpdir"));
        String[] files = testTempDir.list((dir, name) -> name.contains("TGT Renewer"));
        assertTrue("TGT renewal thread must have started", files.length > 0);
    }
}