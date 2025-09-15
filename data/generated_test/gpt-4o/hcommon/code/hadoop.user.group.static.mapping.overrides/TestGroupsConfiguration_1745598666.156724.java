package org.apache.hadoop.security; 

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.apache.hadoop.util.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;
import static org.junit.Assert.*;
import com.google.common.cache.Cache;

import java.util.List;

public class TestGroupsConfiguration {
   
    // Prepare the input conditions for unit testing.
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

        // Verify the static mappings are properly initialized and effective
        Cache<String, List<String>> groupCache = groups.getGroupCache();
        assertNotNull(groupCache); // Verify the cache is not null
        assertFalse(groupCache.asMap().isEmpty()); // Verify the cache has entries

        // Additional verification for static mapping correctness
        try {
            // Static map should correctly resolve `user1` and `user2`
            List<String> groupsForUser1 = groupCache.getIfPresent("user1");
            assertNotNull(groupsForUser1);
            assertEquals(2, groupsForUser1.size());
            assertTrue(groupsForUser1.contains("group1"));
            assertTrue(groupsForUser1.contains("group2"));

            List<String> groupsForUser2 = groupCache.getIfPresent("user2");
            assertNotNull(groupsForUser2);
            assertEquals(1, groupsForUser2.size());
            assertTrue(groupsForUser2.contains("group2"));

        } catch (Exception e) {
            fail("Exception while verifying group mappings: " + e.getMessage());
        }
    }
}