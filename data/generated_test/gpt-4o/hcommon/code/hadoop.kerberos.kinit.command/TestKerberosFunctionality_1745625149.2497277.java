package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;
import javax.security.auth.Subject;
import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestKerberosFunctionality {
  
    // Validate the usage of the `hadoop.kerberos.kinit.command` configuration value
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Step 1: Prepare the configuration with the kinit command
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Create a spy for testing internal method invocation
        KDiag diag = spy(new KDiag(conf));
        
        // Step 3: Invoke the validateKinitExecutable method
        diag.validateKinitExecutable();

        // Step 4: Verify the configuration was accessed and execution occurred
        verify(diag).getConf();

        // Step 5: Ensure kinit validation via file checks
        File kinitPath = new File("/usr/bin/kinit");
        verify(diag).verifyFileIsValid(kinitPath, KDiag.CAT_KERBEROS, KDiag.KERBEROS_KINIT_COMMAND);
    }

    // Test the entire `execute` functionality with configuration propagation and validation
    @Test
    public void testExecuteConfigurationValidation() throws Exception {
        // Step 1: Prepare the input configuration with Kerberos settings
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");
        conf.set("hadoop.security.authentication", "kerberos");

        // Step 2: Create the KDiag instance and execute diagnostics
        KDiag diag = new KDiag(conf);
        boolean result = diag.execute();

        // Step 3: Assert that the execution completes successfully
        assertTrue(result);
    }

    // Test the spawning of Kerberos renewal thread via `spawnAutoRenewalThreadForUserCreds`
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Step 1: Prepare the configuration to enable Kerberos authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Set the configuration and retrieve the login user
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = spy(UserGroupInformation.getLoginUser());

        // Step 3: Invoke the renewal thread spawning method
        ugi.spawnAutoRenewalThreadForUserCreds();

        // Step 4: Verify that Kerberos credentials renewal behavior occurs
        verify(ugi, atLeastOnce()).getTGT();
    }

    // Test UserGroupInformation's `loginUserFromSubject` functionality
    @Test
    public void testLoginUserFromSubject() throws Exception {
        // Step 1: Prepare the configuration for Kerberos and authentication
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Set up Kerberos configuration in UserGroupInformation
        UserGroupInformation.setConfiguration(conf);

        // Step 3: Invoke loginUserFromSubject with a new Subject
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        // Step 4: Verify that the auto-renewal thread is initiated
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        verify(ugi).spawnAutoRenewalThreadForUserCreds();
    }
}