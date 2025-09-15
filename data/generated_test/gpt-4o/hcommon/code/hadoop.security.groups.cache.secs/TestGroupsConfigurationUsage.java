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

public class TestGroupsConfigurationUsage {

    private Groups groups;
    private Configuration conf;

    // Prepare the input conditions for unit testing.
    @Before
    public void setUp() throws Exception {
        // Initialize and set configuration
        conf = new Configuration();
        // Explicit configuration propagation for testing
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300);

        // Instantiate Groups using the configuration
        groups = spy(new Groups(conf));
    }

    @Test
    public void testGetGroupsReturnsCorrectGroups() throws IOException {
        // Validate that the configuration value is loaded correctly
        long expectedCacheTimeout = 300 * 1000; // 300 seconds in milliseconds
        assertEquals(expectedCacheTimeout, groups.getCacheTimeout()); // Ensure the cache timeout is set properly

        // Mock group provider result for a specific user
        String testUser = "testUser";
        List<String> expectedGroups = Arrays.asList("group1", "group2");
        doReturn(expectedGroups).when(groups).getGroupsFromProvider(testUser);

        // First call: groups are fetched and cached
        List<String> returnedGroupsFirstCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsFirstCall);
        verify(groups).getGroupsFromProvider(testUser); // Ensure groups are loaded into the cache

        // Second call: groups are retrieved from cache
        List<String> returnedGroupsSecondCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsSecondCall);
        verify(groups, times(1)).getGroupsFromProvider(testUser); // Provider should only be invoked once
    }

    @Test
    public void testRefreshClearsCache() throws IOException {
        // Mock group provider result for a specific user
        String testUser = "testUser";
        List<String> expectedGroups = Arrays.asList("group1", "group2");
        doReturn(expectedGroups).when(groups).getGroupsFromProvider(testUser);

        // First call: groups are fetched and cached
        List<String> returnedGroupsFirstCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsFirstCall);
        verify(groups).getGroupsFromProvider(testUser); // Ensure groups are loaded into the cache

        // Refresh the group cache
        groups.refresh();
        verify(groups).refresh();

        // After refresh, ensure the provider is invoked again
        List<String> returnedGroupsAfterRefresh = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsAfterRefresh);
        verify(groups, times(2)).getGroupsFromProvider(testUser); // Provider should be called again after refresh
    }

    @Test
    public void testConfigurationPropagation() {
        // Validate that the configuration value is correctly propagated
        long expectedCacheTimeout = conf.getLong(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT) * 1000;
        assertEquals(expectedCacheTimeout, groups.getCacheTimeout());
    }
}