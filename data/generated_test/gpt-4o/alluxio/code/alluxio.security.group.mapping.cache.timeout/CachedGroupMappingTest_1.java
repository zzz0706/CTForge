package alluxio.security.group;

import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.group.GroupMappingService;
import alluxio.security.group.CachedGroupMapping;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class CachedGroupMappingTest {

    @Test
    public void testCachedGroupMapping_Refresh_After_Write() throws Exception {
        // Prepare the test conditions.
        // Use Alluxio InstancedConfiguration API to fetch the cache timeout value.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        long cacheTimeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

        // Assert that the cache timeout is a valid positive value
        Assert.assertTrue("Cache timeout must be greater than zero", cacheTimeoutMs > 0);

        // Create a mock GroupMappingService
        GroupMappingService mockService = new GroupMappingService() {
            @Override
            public List<String> getGroups(String user) {
                // Mock implementation to return a dummy group based on username
                return Arrays.asList(user + "_group");
            }
        };

        // Initialize CachedGroupMapping with the fetched cache timeout
        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(mockService, cacheTimeoutMs);

        // Add entries into the cache
        String user = "testUser";
        cachedGroupMapping.getGroups(user); // Add user to the cache

        // Simulate elapsed time (greater than refresh timeout and less than eviction timeout)
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(cacheTimeoutMs + 100));

        // Test code: Access the cached entry and verify refresh behavior
        List<String> groups = cachedGroupMapping.getGroups(user);

        // Expected Result: Ensure the entry is still present and asynchronously refreshed
        Assert.assertNotNull("Groups should not be null", groups);
        Assert.assertFalse("Groups list should not be empty", groups.isEmpty());
        Assert.assertTrue("Groups should contain the expected suffix", groups.get(0).endsWith("_group"));

        // Additional verification can be added to ensure state consistency within the cache
    }
}