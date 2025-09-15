package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.exception.HadoopIllegalArgumentException;
import org.junit.Test;
import static org.junit.Assert.*;
import com.google.common.cache.Cache;

import java.util.List;

public class TestGroupsConfiguration {
    // Test the Groups constructor and validate its proper use of configuration
    @Test
    public void testGroupsConstructorWithStaticMapping() {
        // Create and configure the mock configuration object
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "user1=group1,group2;user2=group2");

        // Mock Timer instance for testing
        Timer timer = new Timer();

        // Instantiate Groups with the provided configuration and timer
        Groups groups = new Groups(conf, timer);

        // Validate that the cache is initialized
        Cache<String, List<String>> groupCache = groups.getGroupCache();
        assertNotNull(groupCache); // Verify the cache is not null

        // Ensure static mappings are parsed and populated
        List<String> groupsForUser1 = groups.getGroups("user1");
        assertNotNull(groupsForUser1);
        assertEquals(2, groupsForUser1.size());
        assertTrue(groupsForUser1.contains("group1"));
        assertTrue(groupsForUser1.contains("group2"));

        List<String> groupsForUser2 = groups.getGroups("user2");
        assertNotNull(groupsForUser2);
        assertEquals(1, groupsForUser2.size());
        assertTrue(groupsForUser2.contains("group2"));
    }

    // Test invalid configuration scenarios to ensure robustness of parseStaticMapping
    @Test
    public void testGroupsConstructorWithInvalidMapping() {
        // Create and configure the mock configuration with invalid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "invalid_mapping_format");

        // Mock Timer instance for testing
        Timer timer = new Timer();

        try {
            // Instantiate Groups with invalid configuration
            new Groups(conf, timer);
            fail("Expected HadoopIllegalArgumentException due to invalid mapping format");
        } catch (HadoopIllegalArgumentException e) {
            // Expected exception, ensure the error message contains relevant context
            assertTrue(e.getMessage().contains(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES));
        }
    }

    // Test default behavior when no static mapping is provided
    @Test
    public void testGroupsConstructorWithNoStaticMapping() {
        // Create a configuration without static mapping
        Configuration conf = new Configuration();

        // Mock Timer instance for testing
        Timer timer = new Timer();

        // Instantiate Groups without overriding static mappings
        Groups groups = new Groups(conf, timer);

        // Validate that no exceptions are thrown and the cache is empty
        Cache<String, List<String>> groupCache = groups.getGroupCache();
        assertNotNull(groupCache);
        assertTrue(groupCache.asMap().isEmpty());
    }
}