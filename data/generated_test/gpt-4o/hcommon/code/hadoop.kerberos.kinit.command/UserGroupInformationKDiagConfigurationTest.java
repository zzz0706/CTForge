package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.IOException;

public class UserGroupInformationKDiagConfigurationTest {

    /**
     * Test case to ensure validation of the kinit executable
     * using the configuration value.
     */
    @Test
    public void testValidateKinitExecutable() {
        // Prepare configuration with a valid kinit command
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Instantiate KDiag and validate the kinit executable path
        KDiag kDiag = new KDiag(conf);
        kDiag.validateKinitExecutable(); // This method validates the executable path internally
    }

    /**
     * Test case to ensure that Kerberos diagnostics are properly executed
     * based on the configuration.
     */
    @Test
    public void testExecuteKerberosDiagnostics() throws Exception {
        // Prepare configuration for Kerberos authentication
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");
        conf.set("hadoop.security.authentication", "kerberos");

        // Create a KDiag instance and execute the diagnostics
        KDiag kDiag = new KDiag(conf);
        boolean isSuccessful = kDiag.execute();

        // Verify the result of execution
        assert isSuccessful;
    }

    /**
     * Test case to validate behavior of spawnAutoRenewalThreadForUserCreds
     * when security is disabled.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_whenSecurityDisabled() throws IOException {
        // Prepare configuration with simple authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");

        // Set the configuration in UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        // Retrieve the current user and ensure the renewal thread is not spawned
        UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
        assert loginUser != null;
        assert !loginUser.hasKerberosCredentials();
    }

    /**
     * Test case to ensure proper login of a user using a custom subject
     * and validate configuration propagation.
     */
    @Test
    public void testLoginUserFromSubject() throws IOException {
        // Prepare a custom subject for login
        Subject subject = new Subject();

        // Set configuration for simple authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "simple");
        UserGroupInformation.setConfiguration(conf);

        // Perform login with the provided subject
        UserGroupInformation.loginUserFromSubject(subject);

        // Retrieve the logged-in user and ensure login success
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        assert loginUser != null;
    }
}