package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.List;

public class TestGroups {

    // Prepare the input conditions for unit testing.
    @Test
    public void test_refresh_clearsCache_correctly() throws IOException {
        // Step 1: Initialize a Configuration object and get the configuration value using API.
        Configuration conf = new Configuration();
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
}