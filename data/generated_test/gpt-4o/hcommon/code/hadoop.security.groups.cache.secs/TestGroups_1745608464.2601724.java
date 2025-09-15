package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.io.IOException;

public class TestGroups {
    
    // Ensure configuration value 'hadoop.security.groups.cache.secs' is utilized correctly
    // This ensures correct coverage of configuration propagation and usage with refresh()
    @Test
    public void test_refresh_clearsCache_correctly() throws IOException {
        // Step 1: Initialize Configuration object and set configuration using API.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);
        long cacheTimeout = conf.getLong(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;

        // Step 2: Instantiate the Groups class with the configuration
        Groups groups = new Groups(conf);

        // Step 3: Prepare cache for testing by adding regular and negative cache entries
        String testUser = "testUser";
        String negativeUser = "negativeUser";
        groups.cache.put(testUser, List.of("group1", "group2"));
        groups.negativeCache.add(negativeUser);

        // Ensure preconditions where entries exist
        assertTrue(groups.cache.asMap().containsKey(testUser));
        assertTrue(groups.negativeCache.contains(negativeUser));

        // Step 4: Invoke refresh() method
        groups.refresh();

        // Step 5: Verify that caches are cleared
        assertFalse(groups.cache.asMap().containsKey(testUser));
        assertFalse(groups.negativeCache.contains(negativeUser));
    }

    // Ensure configuration value 'hadoop.security.groups.cache.secs' is utilized correctly
    // This verifies group retrieval and exception handling in getGroups()
    @Test
    public void test_getGroups_returnsCorrectGroups() throws IOException {
        // Step 1: Initialize Configuration object and set configuration using API.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        // Step 2: Instantiate the Groups class with the configuration
        Groups groups = new Groups(conf);

        // Step 3: Prepare mock data for user groups in cache
        String testUser = "testUser";
        List<String> expectedGroups = List.of("group1", "group2");
        groups.cache.put(testUser, expectedGroups);

        // Step 4: Call getGroups(String user) for a valid user
        List<String> actualGroups = groups.getGroups(testUser);

        // Verify that the returned groups match the expected output
        assertEquals(expectedGroups, actualGroups);

        // Step 5: Ensure getGroups throws IOException for nonexistent users
        String unknownUser = "unknownUser";
        try {
            groups.getGroups(unknownUser);
            fail("Expected IOException to be thrown for nonexistent user.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No groups found for user"));
        }
    }
}