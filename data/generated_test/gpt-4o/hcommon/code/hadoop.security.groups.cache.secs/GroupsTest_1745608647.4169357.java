package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.junit.Test;
import java.io.IOException;
import java.util.List;

public class GroupsTest {
    // Test the refresh method to ensure configuration-based cache behaviors.
    @Test
    public void testRefreshClearsCache() throws IOException {
        // Initialize a Configuration object and set hadoop.security.groups.cache.secs
        Configuration conf = new Configuration();
        conf.setLong("hadoop.security.groups.cache.secs", 300); // 300 seconds cache timeout

        // Instantiate the Groups class with the configuration
        Groups groups = new Groups(conf);

        // Mock actions to fill the cache
        String testUser1 = "user1";
        String testUser2 = "user2";
        groups.addToNegativeCache(testUser1);
        groups.addToNegativeCache(testUser2);

        // Refresh the Groups cache
        groups.refresh();

        // Verify that cache and negative cache are cleared
        boolean exceptionThrownUser1 = false;
        boolean exceptionThrownUser2 = false;

        try {
            groups.getGroups(testUser1);
        } catch (IOException e) {
            exceptionThrownUser1 = true;
        }

        try {
            groups.getGroups(testUser2);
        } catch (IOException e) {
            exceptionThrownUser2 = true;
        }

        // Assert that both users in the negative cache throw IOException
        assert exceptionThrownUser1;
        assert exceptionThrownUser2;
    }

    // Test the getGroups method to ensure negative cache handling and configuration-based behaviors
    @Test
    public void testGetGroupsHandlesNegativeCache() throws IOException {
        // Initialize a Configuration object and set hadoop.security.groups.cache.secs
        Configuration conf = new Configuration();
        conf.setLong("hadoop.security.groups.cache.secs", 300); // 300 seconds cache timeout

        // Instantiate the Groups class with the configuration
        Groups groups = new Groups(conf);

        // Add a user explicitly to the negative cache
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