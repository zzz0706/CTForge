package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.exception.HadoopIllegalArgumentException;
import org.junit.Test;
import static org.junit.Assert.*;
import java.util.List;
import java.util.concurrent.TimeUnit;
import com.google.common.cache.CacheBuilder;

public class TestGroupsConfigurationEnhanced {

    // Test parseStaticMapping method indirectly via the Groups constructor
    @Test
    public void testGroupsConstructorWithValidStaticMapping() {
        // Prepare Configuration with static mapping property
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1,group2;user2=group2");

        // Mock Timer for testing constructor behavior
        Timer timer = new Timer();

        // Instantiate Groups
        Groups groups = new Groups(conf, timer);

        // Validate group resolution using static mappings set in configuration
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

    // Test handling of invalid static mapping in Groups constructor
    @Test
    public void testGroupsConstructorWithInvalidStaticMapping() {
        // Configure an invalid static mapping
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1;user2==group2");

        // Mock Timer for exception validation
        Timer timer = new Timer();

        try {
            // Attempt instantiation with an invalid mapping
            new Groups(conf, timer);
            fail("Expected HadoopIllegalArgumentException");
        } catch (HadoopIllegalArgumentException e) {
            // Validate if the exception references the property key
            assertTrue(e.getMessage().contains(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES));
        }
    }

    // Test Groups constructor when no static mapping is provided
    @Test
    public void testGroupsConstructorWithNoStaticMapping() {
        Configuration conf = new Configuration();

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate no static mappings and empty cache
        assertNotNull(groups.getGroupCache());
        assertTrue(groups.getGroupCache().asMap().isEmpty());
    }

    // Test resolution of unmapped users when static mapping is provided
    @Test
    public void testGroupsWithUnmappedUsers() {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user1=group1");

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate empty list for unmapped user 'user2'
        List<String> unmappedUserGroups = groups.getGroups("user2");
        assertNotNull(unmappedUserGroups);
        assertTrue(unmappedUserGroups.isEmpty());
    }

    // Test caching behavior of static mappings
    @Test
    public void testGroupsCacheWithStaticMapping() throws Exception {
        Configuration conf = new Configuration();
        conf.set(CommonConfigurationKeys.HADOOP_USER_GROUP_STATIC_OVERRIDES, "user3=group3,group4");
        conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 10);

        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate resolution and caching for 'user3'
        List<String> cachedGroupsForUser = groups.getGroups("user3");
        assertNotNull(cachedGroupsForUser);
        assertEquals(2, cachedGroupsForUser.size());
        assertTrue(cachedGroupsForUser.contains("group3"));
        assertTrue(cachedGroupsForUser.contains("group4"));
    }

    // Test Groups constructor exception-free behavior with default configuration
    @Test
    public void testGroupsConstructorDefaultConfiguration() {
        Configuration conf = new Configuration();
        Timer timer = new Timer();

        Groups groups = new Groups(conf, timer);

        // Validate that cache exists
        assertNotNull(groups.getGroupCache());

        // Ensure staticMapRef is null as no mapping overrides have been set
        assertTrue(groups.getGroupCache().asMap().isEmpty());
    }
}