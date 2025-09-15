package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.List;
import java.io.IOException;

public class TestGroups {
    /**
     * Test refresh() method functionality
     * Verify that it clears both the regular cache and the negative cache.
     */
    @Test
    public void test_refresh_clearsCache_correctly() throws IOException {
        // Step 1: Initialize Configuration object and set configuration using API
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);
        
        // Step 2: Instantiate the Groups class using the configuration
        Groups groups = new Groups(conf);

        // Step 3: Prepare the cache conditions for testing
        String testUser = "testUser";
        String negativeUser = "negativeUser";
        groups.cache.put(testUser, List.of("group1", "group2"));
        groups.negativeCache.add(negativeUser);

        // Ensure the preconditions where entries exist
        assertTrue(groups.cache.asMap().containsKey(testUser));
        assertTrue(groups.negativeCache.contains(negativeUser));

        // Step 4: Invoke refresh() method
        groups.refresh();

        // Step 5: Validate that both caches are cleared after refresh()
        assertFalse(groups.cache.asMap().containsKey(testUser));
        assertFalse(groups.negativeCache.contains(negativeUser));
    }

    /**
     * Test getGroups(String user) functionality
     * Verify correct group memberships are returned for valid users and exceptions for nonexistent users.
     */
    @Test
    public void test_getGroups_returnsCorrectGroups() throws IOException {
        // Step 1: Initialize Configuration object and set configuration using API
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        // Step 2: Instantiate the Groups class using the configuration
        Groups groups = new Groups(conf);

        // Step 3: Add mock data for user group mapping to the cache
        String testUser = "testUser";
        List<String> expectedGroups = List.of("group1", "group2");
        groups.cache.put(testUser, expectedGroups);

        // Step 4: Verify group memberships for a valid user
        List<String> actualGroups = groups.getGroups(testUser);
        assertEquals(expectedGroups, actualGroups);

        // Step 5: Verify that IOException is thrown for nonexistent users
        String unknownUser = "unknownUser";
        try {
            groups.getGroups(unknownUser);
            fail("Expected IOException to be thrown for nonexistent user.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No groups found for user"));
        }
    }
}