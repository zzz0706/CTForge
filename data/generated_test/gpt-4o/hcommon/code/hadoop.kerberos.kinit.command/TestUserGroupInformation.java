package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;
import javax.security.auth.Subject;
import static org.junit.Assert.*;

public class TestUserGroupInformation {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testLoginUserFromSubject_withInvalidSubject() {
        // Initialize the Hadoop configuration
        Configuration conf = new Configuration();

        // Simulate Kerberos being enabled
        conf.set("hadoop.security.authentication", "kerberos");

        // Set the UserGroupInformation configuration
        UserGroupInformation.setConfiguration(conf);

        try {
            // Invoke the loginUserFromSubject method with a null Subject
            UserGroupInformation.loginUserFromSubject(null);
            fail("Expected IOException due to invalid Subject");
        } catch (IOException e) {
            // Verify that the appropriate exception is raised
            assertTrue(e.getMessage().contains("failure to login"));
        }
    }
}