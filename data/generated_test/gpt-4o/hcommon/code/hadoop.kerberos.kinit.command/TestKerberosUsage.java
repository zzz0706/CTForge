package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KDiag;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import javax.security.auth.Subject;
import java.io.IOException;

public class TestKerberosUsage {

    /**
     * Test case: testValidateKinitExecutable_withRelativePath
     * Test the function validateKinitExecutable with a relative path.
     */
    @Test
    public void testValidateKinitExecutable_withRelativePath() {
        // Get configuration value using API
        Configuration config = new Configuration();
        config.set(KDiag.KERBEROS_KINIT_COMMAND, "kinit");

        // Prepare the input conditions for unit testing
        KDiag kDiag = new KDiag(config);

        // Test code
        kDiag.validateKinitExecutable();
    }

    /**
     * Test case: testExecute_withValidConfiguration
     * Test the execute function of KDiag class.
     */
    @Test
    public void testExecute_withValidConfiguration() throws Exception {
        // Get configuration value using API
        Configuration config = new Configuration();
        config.set(KDiag.KERBEROS_KINIT_COMMAND, "kinit");

        // Prepare the input conditions for unit testing
        KDiag kDiag = new KDiag(config);

        // Test code
        boolean result = kDiag.execute();

        // Verify results
        assert result : "Execution returned false, check diagnostics.";
    }

    /**
     * Test case: testSpawnAutoRenewalThreadForUserCreds_withValidConfiguration
     * Test if the function spawnAutoRenewalThreadForUserCreds successfully spawns the thread.
     */
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_withValidConfiguration() {
        // Get configuration value using API
        Configuration config = new Configuration();
        config.set(UserGroupInformation.HADOOP_KERBEROS_KINIT_COMMAND, "kinit");

        // Prepare the input conditions for unit testing
        UserGroupInformation mockUserGroupInformation = 
            UserGroupInformation.createRemoteUser("test-user");
        mockUserGroupInformation.setConfiguration(config);

        // Test code
        mockUserGroupInformation.spawnAutoRenewalThreadForUserCreds();
    }

    /**
     * Test case: testLoginUserFromSubject_withValidSubject
     * Test loginUserFromSubject function with a valid subject.
     */
    @Test
    public void testLoginUserFromSubject_withValidSubject() throws IOException {
        // Get configuration value using API
        Configuration config = new Configuration();
        config.set("hadoop.kerberos.kinit.command", "kinit");

        // Prepare the input conditions for unit testing
        Subject mockSubject = new Subject();

        // Test code
        UserGroupInformation.loginUserFromSubject(mockSubject);

        // Verify results
        UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
        assert loginUser != null : "Login user is null.";
    }
}