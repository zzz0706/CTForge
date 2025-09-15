package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;

public class TestGroups {

    // Test that the `refresh()` method clears both the regular cache and the negative cache.
    @Test
    public void test_refresh_clearsCache_correctly() throws IOException {
        // Step 1: Initialize a Configuration object and get the configuration value using API.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);
        long cacheTimeout = conf.getLong(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;

        // Step 2: Instantiate the Groups class with the configuration.
        Groups groups = new Groups(conf);

        // Step 3: Add entries to the cache and negative cache explicitly.
        String testUser = "testUser";
        String negativeUser = "negativeUser";
        groups.cache.put(testUser, List.of("group1", "group2"));
        groups.negativeCache.add(negativeUser);

        // Verify that the entries exist before calling refresh().
        assertTrue(groups.cache.asMap().containsKey(testUser));
        assertTrue(groups.negativeCache.contains(negativeUser));

        // Step 4: Invoke the refresh() method.
        groups.refresh();

        // Step 5: Verify that the caches are cleared.
        assertFalse(groups.cache.asMap().containsKey(testUser));
        assertFalse(groups.negativeCache.contains(negativeUser));
    }

    // Test that `getGroups(String user)` retrieves group information correctly or handles exceptions.
    @Test
    public void test_getGroups_returnsCorrectGroups() throws IOException {
        // Step 1: Initialize a Configuration object and get the configuration value using API.
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        // Step 2: Instantiate the Groups class with the configuration.
        Groups groups = new Groups(conf);

        // Step 3: Set up mock data.
        String testUser = "testUser";
        List<String> expectedGroups = List.of("group1", "group2");
        groups.cache.put(testUser, expectedGroups);

        // Step 4: Call `getGroups(String user)` for an existing user.
        List<String> actualGroups = groups.getGroups(testUser);

        // Step 5: Verify that the returned group list matches the expected result.
        assertEquals(expectedGroups, actualGroups);

        // Step 6: Test with a user that does not exist, verify IOException is thrown.
        String unknownUser = "unknownUser";
        try {
            groups.getGroups(unknownUser);
            fail("Expected IOException to be thrown for unknown user.");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("No groups found for user"));
        }
    }
}