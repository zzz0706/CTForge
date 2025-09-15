package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.apache.hadoop.security.Groups;
import java.io.IOException;

public class GroupsTest {
    // get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void test_getGroups_handlesNegativeCache() throws IOException {
        // Initialize a Configuration object
        Configuration conf = new Configuration();
        
        // Instantiate the Groups class with the configuration
        Groups groups = new Groups(conf);

        // Add a user to the negative cache
        String testUser = "negativeUser";
        groups.addToNegativeCache(testUser);
        
        // Call getGroups() for the user added to the negative cache
        boolean exceptionThrown = false;
        try {
            groups.getGroups(testUser);
        } catch (IOException e) {
            exceptionThrown = true;
            // Ensure the exception message indicates no groups for the user
            assert e.getMessage().contains("No groups for user");
        }

        // Verify that IOException was thrown
        assert exceptionThrown;
    }
}