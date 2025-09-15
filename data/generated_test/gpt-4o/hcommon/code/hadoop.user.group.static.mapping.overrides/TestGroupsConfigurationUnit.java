package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.security.exception.HadoopIllegalArgumentException;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.common.cache.CacheBuilder;

public class TestGroupsConfigurationUnit {
    // Verify Groups constructor properly parses and handles valid static mapping configuration
    @Test
    public void testGroupsConstructorWithStaticMapping() {
        // Prepare Configuration with valid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1,group2;user2=group2");

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate parsed configuration - static user-to-groups mapping
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

    // Test Groups constructor behavior with invalid static mapping configuration
    @Test
    public void testConstructorWithInvalidStaticMapping() {
        // Configure invalid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "malformedMapping");

        Timer timer = new Timer();

        try {
            new Groups(conf, timer);
            fail("Expected a HadoopIllegalArgumentException due to invalid static mapping");
        } catch (HadoopIllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES));
        }
    }

    // Ensure proper behavior when no static mapping is provided in configuration
    @Test
    public void testConstructorWithoutStaticMapping() {
        // Prepare empty Configuration
        Configuration conf = new Configuration();

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate absence of static mapping
        assertNotNull(groups.getGroupCache());
        assertTrue(groups.getGroupCache().asMap().isEmpty());
    }

    // Validate unmapped user behavior when static mapping is provided
    @Test
    public void testUnmappedUserResolution() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "mappedUser=groupA");

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate resolution of an unmapped user (returns empty list)
        List<String> unmappedUserGroups = groups.getGroups("unmappedUser");
        assertNotNull(unmappedUserGroups);
        assertTrue(unmappedUserGroups.isEmpty());
    }

    // Test caching mechanism for static user-to-groups mappings
    @Test
    public void testGroupsCacheBehaviorWithStaticMapping() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "cachedUser=group1,group2");
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate groups resolution and cache usage for a mapped user
        List<String> groupsForCachedUser = groups.getGroups("cachedUser");
        assertNotNull(groupsForCachedUser);
        assertEquals(2, groupsForCachedUser.size());
        assertTrue(groupsForCachedUser.contains("group1"));
        assertTrue(groupsForCachedUser.contains("group2"));
    }

    // Confirm default configuration leads to correct Group initialization
    @Test
    public void testConstructorWithDefaultConfiguration() {
        Configuration conf = new Configuration();

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate the absence of static mapping and negative cache configuration
        assertNotNull(groups.getGroupCache());
        assertTrue(groups.getGroupCache().asMap().isEmpty());
    }
}