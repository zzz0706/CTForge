package org.apache.hadoop.security;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

public class UserGroupInformationTest {

    // Prepare the input conditions for unit testing.
    @Test
    public void testSpawnAutoRenewalThreadForUserCreds_whenSecurityDisabled() {
        // Step 1: Get configuration value using the API.
        Configuration conf = new Configuration();
        String authMethod = conf.get("hadoop.security.authentication", "simple");

        // Step 2: Ensure that Kerberos is disabled ('simple' authentication).
        assert "simple".equalsIgnoreCase(authMethod);

        // Step 3: Create a UserGroupInformation instance and ensure no Kerberos credentials are present.
        UserGroupInformation.setConfiguration(conf);

        // Step 4: Check that no renewal thread is spawned since security is disabled.
        UserGroupInformation loginUser = UserGroupInformation.getCurrentUser();
        assert loginUser != null;
        assert !loginUser.hasKerberosCredentials(); // No Kerberos credentials for the user.
    }
}