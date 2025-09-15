package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.security.HadoopIllegalArgumentException;
import org.junit.Test;

import static org.junit.Assert.*;
import java.util.*;

public class TestGroupsCachingConfiguration {

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testGroupsConstructorWithCachingConfiguration() {
        // Initialize the Configuration object and set caching properties
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 600); // Set custom cache timeout
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 60); // Set custom negative cache timeout

        // Mock Timer instance
        Timer mockTimer = new Timer();

        // Instantiate Groups
        Groups groups = new Groups(conf, mockTimer);

        // Verify that cache timeout is correctly initialized
        long expectedCacheTimeoutMs = 600 * 1000; // 600 seconds in milliseconds
        assertEquals("Cache timeout mismatch", expectedCacheTimeoutMs, groups.getCacheTimeout());

        // Verify that negative cache timeout is correctly initialized
        long expectedNegativeCacheTimeoutMs = 60 * 1000; // 60 seconds in milliseconds
        if (expectedNegativeCacheTimeoutMs > 0) {
            assertTrue("Negative cache timeout must be positive", groups.hasNegativeCache());
        } else {
            assertFalse("Negative cache should not exist", groups.hasNegativeCache());
        }
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testParseStaticMapping() {
        // Initialize the Configuration object and set static mapping property
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1,group2;user2=group3");

        // Mock Timer instance
        Timer mockTimer = new Timer();

        // Instantiate Groups
        Groups groups = new Groups(conf, mockTimer);

        // Reflectively access the staticMapRef (simplified assertion since staticMapRef is private)
        Map<String, List<String>> staticMappings = groups.getStaticMappings(); // Hypothetical API for testing

        // Verify that static mappings are parsed and correctly populated
        assertNotNull("Static mappings should not be null", staticMappings);
        assertEquals("Incorrect number of mappings", 2, staticMappings.size());
        assertEquals("Incorrect groups for user1", Arrays.asList("group1", "group2"), staticMappings.get("user1"));
        assertEquals("Incorrect groups for user2", Collections.singletonList("group3"), staticMappings.get("user2"));
    }

    // Get configuration value using API
    // Prepare the input conditions for unit testing.
    @Test
    public void testParseStaticMappingWithMalformedInput() {
        // Initialize the Configuration object and set malformed static mapping property
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1,group2;user2");

        // Mock Timer instance
        Timer mockTimer = new Timer();

        // Expect that the malformed input throws a HadoopIllegalArgumentException
        try {
            new Groups(conf, mockTimer);
            fail("Expected HadoopIllegalArgumentException due to malformed input");
        } catch (HadoopIllegalArgumentException e) {
            assertTrue("Exception message mismatch", e.getMessage().contains(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES));
        }
    }
}