package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.KDiag;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.File;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

public class TestConfigurationUsage {

    // Validate the `hadoop.kerberos.kinit.command` configuration value
    @Test
    public void testValidateKinitExecutable() throws Exception {
        // Step 1: Configure the `hadoop.kerberos.kinit.command` value
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");

        // Step 2: Prepare the `KDiag` instance for diagnostics
        KDiag diag = spy(new KDiag(conf));

        // Step 3: Invoke the method to validate `kinit` executable
        diag.validateKinitExecutable();

        // Step 4: Verify the method's interaction with configuration and file validation
        File kinitPath = new File("/usr/bin/kinit");
        verify(diag).getConf();
        verify(diag).verifyFileIsValid(kinitPath, KDiag.CAT_KERBEROS, KDiag.KERBEROS_KINIT_COMMAND);
    }

    // Ensure coverage of configuration propagation in `KDiag.execute()`
    @Test
    public void testExecuteConfigurationValidation() throws Exception {
        // Step 1: Configure Hadoop security settings with Kerberos enabled
        Configuration conf = new Configuration();
        conf.set(KDiag.KERBEROS_KINIT_COMMAND, "/usr/bin/kinit");
        conf.set("hadoop.security.authentication", "kerberos");

        // Step 2: Create the `KDiag` instance and execute diagnostics
        KDiag diag = new KDiag(conf);
        boolean result = diag.execute();

        // Step 3: Verify the execution result
        assertTrue(result);
    }

    // Test `spawnAutoRenewalThreadForUserCreds` for Kerberos renewal thread
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds() throws Exception {
        // Step 1: Configure Kerberos authentication for Hadoop security
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Set configuration values for `UserGroupInformation` and retrieve the login user
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation ugi = spy(UserGroupInformation.getLoginUser());

        // Step 3: Invoke method responsible for spawning the renewal thread
        ugi.spawnAutoRenewalThreadForUserCreds();

        // Step 4: Verify that the method accesses Kerberos credentials appropriately
        verify(ugi, atLeastOnce()).getTGT();
    }

    // Verify functionality of `loginUserFromSubject` with Kerberos configuration
    @Test
    public void testLoginUserFromSubject() throws Exception {
        // Step 1: Configure security settings with Kerberos authentication enabled
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.kerberos.kinit.command", "/usr/bin/kinit");

        // Step 2: Apply the configuration with `UserGroupInformation`
        UserGroupInformation.setConfiguration(conf);

        // Step 3: Create a new `Subject` and log in
        Subject subject = new Subject();
        UserGroupInformation.loginUserFromSubject(subject);

        // Step 4: Verify auto-renewal threading behavior post-login
        UserGroupInformation ugi = UserGroupInformation.getLoginUser();
        verify(ugi).spawnAutoRenewalThreadForUserCreds();
    }
}