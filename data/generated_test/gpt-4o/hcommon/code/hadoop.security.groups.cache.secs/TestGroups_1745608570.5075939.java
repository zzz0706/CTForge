package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TestGroups {

    private Groups groups;
    private Configuration conf;

    // Prepare the input conditions for unit testing.
    @Before
    public void setUp() throws Exception {
        // Initialize and set configuration
        conf = new Configuration();
        // Explicit configuration propagation for testing
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300);

        // Instantiate and mock necessary classes
        groups = spy(new Groups(conf));
    }

    @Test
    public void test_getGroups_configurationUsageAndCaching() throws IOException {
        // Ensure that the configuration value is propagated correctly
        long expectedCacheTimeout = 300 * 1000; // 300 seconds converted to milliseconds
        assertEquals(expectedCacheTimeout, groups.getCacheTimeout());  // Assuming a getter method `getCacheTimeout()` exists for testing or internal use

        // Mock group provider result for a specific user
        String testUser = "testUser";
        List<String> expectedGroups = Arrays.asList("group1", "group2");
        doReturn(expectedGroups).when(groups).getGroupsFromProvider(testUser);

        // First call: confirm retrieval and cache behavior
        List<String> returnedGroupsFirstCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsFirstCall);
        verify(groups).getGroupsFromProvider(testUser); // Ensures loading occurred from provider

        // Second call: confirm usage from the cache
        List<String> returnedGroupsSecondCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsSecondCall);
        verify(groups, times(1)).getGroupsFromProvider(testUser); // Ensures provider is called only once

        // Explicit test of refresh behavior
        groups.refresh(); // Refresh should clear the cache
        verify(groups).refresh(); 

        // After refresh, new cache load should occur
        List<String> returnedGroupsAfterRefresh = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsAfterRefresh);
        verify(groups, times(2)).getGroupsFromProvider(testUser); // Provider is called again after refresh
    }
}