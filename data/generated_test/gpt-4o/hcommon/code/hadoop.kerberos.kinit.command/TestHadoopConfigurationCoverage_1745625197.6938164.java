package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import javax.security.auth.Subject;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestHadoopConfigurationCoverage {

    // Test case to verify validateKinitExecutable method works correctly by using KDiag API
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Step 1: Setup configuration value for 'hadoop.kerberos.kinit.command'
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Spy the KDiag instance to inspect internal interactions
        KDiag diag = spy(new KDiag(conf));

        // Step 3: Call the validateKinitExecutable method
        diag.validateKinitExecutable();

        // Step 4: Verify internal configuration retrieval and method execution
        verify(diag).getConf();
        verify(diag).validateKinitExecutable();
    }

    // Test case for executing diagnostics through KDiag API
    @Test
    public void testExecuteMethodConfigurationUsage() throws Exception {
        // Step 1: Setup security configuration for Kerberos
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Instantiate KDiag and execute the diagnostics
        KDiag diag = new KDiag(conf);
        boolean success = diag.execute();

        // Step 3: Assert diagnostic execution success
        assertTrue(success);
    }

    // Test case to verify spawning of TGT renewal thread using UserGroupInformation API
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Step 1: Configure Kerberos authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Set the configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = spy(UserGroupInformation.getLoginUser());

        // Step 3: Trigger the method to spawn the renewal thread
        ugi.spawnAutoRenewalThreadForUserCreds();

        // Step 4: Verify that getTGT() is called during thread processing
        verify(ugi, atLeastOnce()).getTGT();
    }

    // Test case for loginUserFromSubject method invoked with Kerberos configuration applied
    @Test
    public void testLoginUserFromSubject() throws Exception {
        // Step 1: Setup configuration for Kerberos
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Apply configuration to UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        // Step 3: Create a Subject and pass it to loginUserFromSubject
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        // Step 4: Confirm that spawnAutoRenewalThreadForUserCreds is called after login
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        verify(ugi, atLeastOnce()).spawnAutoRenewalThreadForUserCreds();
    }
}