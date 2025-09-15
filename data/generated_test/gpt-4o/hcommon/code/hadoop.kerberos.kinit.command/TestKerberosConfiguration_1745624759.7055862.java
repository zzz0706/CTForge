package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;
import static org.junit.Assert.*;

public class TestKerberosConfiguration {

    // Test proper functionality of `loginUserFromSubject` and configuration usage.
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws Exception {
        // Get configuration value using the API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "kinit"); // Example setting for testing
        UserGroupInformation.setConfiguration(conf);

        // Prepare valid input for the test
        Subject subject = new Subject();

        // Execute the test scenario
        UserGroupInformation.loginUserFromSubject(subject);

        // Validate result
        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("Verify login method is Kerberos", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());
    }

    // Test the validation of the kinit command through `validateKinitExecutable`.
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Get configuration value using the API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit"); // Example setting

        // Prepare valid input for testing
        KDiag kdiag = new KDiag(conf);

        // Execute the test scenario
        boolean executedSuccessfully = kdiag.execute();

        // Validate result
        assertTrue("KDiag execution should succeed with valid configuration", executedSuccessfully);
    }

    // Test TGT renewal thread spawning through `spawnAutoRenewalThreadForUserCreds`.
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.set("hadoop.kerberos.kinit.command", "kinit"); // Example setting
        UserGroupInformation.setConfiguration(conf);

        // Prepare valid input for testing
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        UserGroupInformation loggedInUser = UserGroupInformation.getLoginUser();
        assertNotNull("Logged-in user should not be null", loggedInUser);
        assertEquals("Verify login method is Kerberos", 
            UserGroupInformation.AuthenticationMethod.KERBEROS, 
            loggedInUser.getAuthenticationMethod());

        // Verify if the TGT renewal thread is properly initialized
        File testTempDir = new File(System.getProperty("java.io.tmpdir"));
        String[] files = testTempDir.list((dir, name) -> name.contains("TGT Renewer"));
        assertTrue("TGT renewal thread must have started", files.length > 0);
    }
}