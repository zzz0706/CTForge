package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.IOException;

public class ConfigurationTestCases {

    /**
     * Test case to ensure proper validation of kinit executable using configuration.
     */
    @Test
    public void testValidateKinitExecutable() {
        // Prepare configuration with valid kinit command
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Instantiate KDiag and validate the kinit executable
        KDiag kDiag = new KDiag(conf);
        kDiag.validateKinitExecutable(); // Validate the executable path
    }

    /**
     * Test case to execute diagnostics in a Kerberos environment and validate configurations.
     */
    @Test
    public void testExecuteKerberosDiagnostics() throws Exception {
        // Prepare configuration for Kerberos
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");
        conf.set("hadoop.security.authentication", "kerberos");

        // Create a KDiag instance and perform diagnostics
        KDiag kDiag = new KDiag(conf);
        boolean isSuccessful = kDiag.execute();

        // Verify the execution result
        assert isSuccessful;
    }

    /**
     * Test case to validate that renewal threads are not spawned when security is disabled.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_whenSecurityDisabled() throws IOException {
        // Prepare configuration with simple authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");

        // Set the configuration in UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        // Retrieve the current user and validate no renewal thread is spawned
        UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
        assert loginUser != null;
        assert !loginUser.hasKerberosCredentials();
    }

    /**
     * Test case for logging in a user using a subject and verifying configurations.
     */
    @Test
    public void testLoginUserFromSubject() throws IOException {
        // Prepare subject for login
        Subject subject = new Subject();

        // Set configuration for simple authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);

        // Perform login with subject
        UserGroupInformation.loginUserFromSubject(subject);

        // Retrieve the logged-in user and validate login success
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        assert loginUser != null;
    }
}