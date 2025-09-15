package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;
import java.io.File;
import static org.mockito.Mockito.*;

public class TestKerberosConfiguration {

    // Validate the usage of configuration value `hadoop.kerberos.kinit.command`
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Step 1: Configure and set Kerberos settings
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit"); // Absolute path for kinit executable

        // Step 2: Mock the necessary KDiag instance
        KDiag diag = spy(new KDiag(conf));

        // Step 3: Execute the `validateKinitExecutable` method to test its behavior
        diag.validateKinitExecutable();

        // Step 4: Verify internal method calls
        verify(diag, atLeastOnce()).getConf();

        // Step 5: Verify kinit file checks
        File kinitPath = new File("/usr/bin/kinit");
        verify(diag).verifyFileIsValid(kinitPath, KDiag.CAT_KERBEROS, KDiag.KERBEROS_KINIT_COMMAND);
    }

    // Validate the execution and configuration propagation in KDiag
    @Test
    public void testExecuteConfigurationPropagation() throws Exception {
        // Step 1: Configure Kerberos and diagnostics
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit"); // Set kinit executable
        conf.set("hadoop.security.authentication", "kerberos"); // Enable Kerberos authentication

        // Step 2: Set up the KDiag instance
        KDiag diag = new KDiag(conf);

        // Step 3: Execute Kerberos diagnostics
        boolean result = diag.execute();

        // Step 4: Verify that the diagnostics execute successfully
        assert result;

        // Step 5: Verify that validateKinitExecutable() was called internally
        verify(privateMethod(diag, "validateKinitExecutable"), times(1));
    }

    // Test the spawning of the TGT auto-renewal thread in UserGroupInformation
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Step 1: Configure Kerberos within Hadoop
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos"); // Set Kerberos authentication
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit"); // Specify the kinit command

        // Step 2: Mock UserGroupInformation
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = spy(UserGroupInformation.getLoginUser());

        // Step 3: Simulate and invoke the spawnAutoRenewalThreadForUserCreds method
        ugi.spawnAutoRenewalThreadForUserCreds();

        // Step 4: Verify that Kerberos credentials renewal behavior occurs
        verify(ugi, atLeastOnce()).getTGT();
    }

    // Test loginUserFromSubject method in UserGroupInformation
    @Test
    public void testLoginUserFromSubject() throws Exception {
        // Step 1: Configure Kerberos within Hadoop
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos"); // Set Kerberos authentication
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit"); // Specify the kinit command

        // Step 2: Set up UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        // Step 3: Simulate a Subject-based login attempt
        UserGroupInformation.loginUserFromSubject(null);

        // Step 4: Verify the auto-renewal thread is spawned correctly
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        verify(privateMethod(ugi, "spawnAutoRenewalThreadForUserCreds"), times(1));
    }
}