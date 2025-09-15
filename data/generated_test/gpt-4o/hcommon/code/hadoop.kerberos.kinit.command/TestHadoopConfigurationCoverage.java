package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestHadoopConfigurationCoverage {

    // Test case to verify validation of Kinit command
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Step 1: Setting configuration value for 'hadoop.kerberos.kinit.command'
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Mock the KDiag class to spy on method calls
        KDiag diag = spy(new KDiag(conf));

        // Step 3: Execute the method responsible for validating Kinit executable
        diag.validateKinitExecutable();

        // Step 4: Verify the interaction with configuration and file validation
        verify(diag).getConf();
        verify(diag).validateKinitExecutable();
    }

    // Ensure configuration propagation and correctness for execute method of KDiag
    @Test
    public void testExecuteMethodConfigurationUsage() throws Exception {
        // Step 1: Setup security configuration for Kerberos enabled
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Instantiate KDiag and execute diagnostics
        KDiag diag = new KDiag(conf);
        boolean success = diag.execute();

        // Step 3: Assert the successful execution result
        assertTrue(success);
    }

    // Test case for spawning renewal thread for Kerberos credentials
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Step 1: Configure Kerberos authentication within the environment
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Apply configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = spy(UserGroupInformation.getLoginUser());

        // Step 3: Trigger the method to spawn renewal thread
        ugi.spawnAutoRenewalThreadForUserCreds();

        // Step 4: Validate usage of Kerberos credentials during thread spawning
        verify(ugi, atLeastOnce()).getTGT();
    }

    // Verify the login functionality with Kerberos configuration in UserGroupInformation
    @Test
    public void testLoginUserFromSubject() throws Exception {
        // Step 1: Configure security settings for Kerberos authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Apply the above configuration to UserGroupInformation APIs
        UserGroupInformation.setConfiguration(conf);

        // Step 3: Log in using Subject API for Kerberos authentication
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        // Step 4: Verify that renewal thread is invoked post-login operation
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        verify(ugi).spawnAutoRenewalThreadForUserCreds();
    }
}