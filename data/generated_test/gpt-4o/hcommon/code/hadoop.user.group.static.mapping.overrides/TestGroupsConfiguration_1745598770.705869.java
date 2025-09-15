package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.exception.HadoopIllegalArgumentException;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;

public class TestGroupsConfiguration {

    // Validate the constructor correctly parses and handles static mappings
    @Test
    public void testGroupsConstructorWithStaticMapping() {
        // Prepare Configuration with static mapping property
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "user1=group1,group2;user2=group2");

        // Mock Timer instance for reliability
        Timer timer = new Timer();

        // Instantiate Groups with configuration
        Groups groups = new Groups(conf, timer);

        // Check if cache was initialized
        assertNotNull(groups.getGroupCache());

        // Validate parsed static mappings through group resolution
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

    // Ensure invalid static mappings trigger appropriate exceptions
    @Test
    public void testGroupsConstructorWithInvalidMapping() {
        // Configure an invalid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "user1;user2==group2");

        // Mock Timer instance for error validation
        Timer timer = new Timer();

        try {
            // Expect exception on invalid configuration
            new Groups(conf, timer);
            fail("Expected HadoopIllegalArgumentException");
        } catch (HadoopIllegalArgumentException e) {
            // Validate exception message context
            assertTrue(e.getMessage().contains(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES));
        }
    }

    // Check handling of default configuration with no static mappings
    @Test
    public void testGroupsConstructorWithNoStaticMapping() {
        // Configure an empty Configuration instance
        Configuration conf = new Configuration();

        // Mock Timer instance for baseline testing
        Timer timer = new Timer();

        // Instantiate Groups without overriding static mappings
        Groups groups = new Groups(conf, timer);

        // Confirm absence of static mappings and empty cache
        assertNotNull(groups.getGroupCache());
        assertTrue(groups.getGroupCache().asMap().isEmpty());
    }

    // Validate group resolution for unmapped users with no static mappings
    @Test
    public void testGroupsWithUnmappedUsers() {
        // Configure a simple mapping with no unmapped users
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "user1=group1");

        // Mock Timer instance
        Timer timer = new Timer();

        // Instantiate Groups with configuration
        Groups groups = new Groups(conf, timer);

        // Check groups for an unmapped user
        List<String> unmappedUserGroups = groups.getGroups("user2");
        assertNotNull(unmappedUserGroups);
        // Ensure the result is an empty list for unmapped users
        assertTrue(unmappedUserGroups.isEmpty());
    }

    // Verify static mapping interaction when caching is enabled
    @Test
    public void testGroupsCacheWithStaticMapping() throws Exception {
        // Configure static mappings and cache parameters
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, 
                 "user3=group3,group4");
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        // Mock Timer instance
        Timer timer = new Timer();

        // Instantiate Groups with configuration
        Groups groups = new Groups(conf, timer);

        // Verify static mappings are cached
        List<String> cachedGroupsForUser = groups.getGroups("user3");
        assertNotNull(cachedGroupsForUser);
        assertEquals(2, cachedGroupsForUser.size());
        assertTrue(cachedGroupsForUser.contains("group3"));
        assertTrue(cachedGroupsForUser.contains("group4"));
    }
}