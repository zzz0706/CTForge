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
        // Set 'hadoop.security.groups.cache.secs' explicitly to verify usage
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300);

        // Instantiate the Groups object to be tested
        groups = spy(new Groups(conf));
    }

    // Test case: Verify that getGroups() correctly uses the cache mechanism
    @Test
    public void test_getGroups_returnsCorrectGroups() throws IOException {
        // Mock the group provider to return simulated groups for a specific user
        String testUser = "testUser";
        List<String> expectedGroups = Arrays.asList("group1", "group2");
        doReturn(expectedGroups).when(groups).getGroupsFromProvider(testUser);

        // First call to getGroups should obtain mappings from the provider and cache them
        List<String> returnedGroupsFirstCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsFirstCall);
        verify(groups).getGroupsFromProvider(testUser); // Assert provider was called

        // Second call to getGroups should retrieve mappings from the cache
        List<String> returnedGroupsSecondCall = groups.getGroups(testUser);
        assertEquals(expectedGroups, returnedGroupsSecondCall);
        verify(groups, times(1)).getGroupsFromProvider(testUser); // Ensure provider call was cached

        // Additional coverage: Test refresh functionality
        groups.refresh(); // Explicitly clear the cache
        verify(groups).refresh(); 
    }
}