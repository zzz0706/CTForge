package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KDiag;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;

import static org.junit.Assert.*;

public class TestKerberosConfigurationEnhancements {

    /**
     * Test the proper functionality and configuration usage of `loginUserFromSubject`.
     */
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "kinit"); // Example setting
        UserGroupInformation.setConfiguration(conf);

        // Prepare input conditions for unit testing
        Subject subject = new Subject();

        // Execute test
        UserGroupInformation.loginUserFromSubject(subject);

        // Validate outcome
        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("AuthenticationMethod must be Kerberos", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());
    }

    /**
     * Test the validation of the `kinit` command configuration in `KDiag`.
     */
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit"); // Example setting

        // Prepare input conditions for unit testing
        KDiag kdiag = new KDiag(conf);

        // Execute test
        boolean executedSuccessfully = kdiag.execute();

        // Validate outcome
        assertTrue("KDiag execution should succeed with valid configuration", executedSuccessfully);
    }

    /**
     * Test the spawning of the TGT renewal thread and its configuration usage.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "kinit"); // Example setting
        UserGroupInformation.setConfiguration(conf);

        // Prepare input conditions for unit testing
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("AuthenticationMethod must be Kerberos", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());

        // Verify if the TGT renewal thread is properly initialized
        // Check for the presence of a thread related to TGT renewal
        File testTempDir = new File(System.getProperty("java.io.tmpdir"));
        String[] files = testTempDir.list((dir, name) -> name.contains("TGT Renewer"));
        assertTrue("TGT renewal thread must have started", files != null && files.length > 0);
    }
}