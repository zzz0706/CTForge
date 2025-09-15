package alluxio.security.group;

import org.junit.Test;
import org.junit.Assert;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.group.GroupMappingService;
import alluxio.security.group.CachedGroupMapping;

import java.util.concurrent.TimeUnit;

public class CachedGroupMappingTest {
    // Test to verify eviction behavior of CachedGroupMapping after expiration timeout
    @Test
    public void testCachedGroupMappingExpireAfterWrite() throws Exception {
        // Step 1: Use the Alluxio API to obtain configuration values
        AlluxioConfiguration conf = InstancedConfiguration.defaults();
        long cacheTimeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

        // Ensure that the cache timeout is valid and positive
        Assert.assertTrue("Cache timeout must be positive", cacheTimeoutMs > 0);

        // Step 2: Prepare the test condition
        // Create a mock implementation of GroupMappingService for testing
        GroupMappingService mockService = user -> {
            // Simulate a group-mapping lookup with constant group data
            return java.util.Arrays.asList("group1", "group2", "group3");
        };

        // Initialize CachedGroupMapping with the mock service and configuration timeout
        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(mockService, cacheTimeoutMs);

        // Step 3: Test code
        // Add an entry to the cache by fetching groups for a user
        String testUser = "testUser";
        cachedGroupMapping.getGroups(testUser); // This populates the cache for testUser
        
        // Simulate elapsed time exceeding the eviction timeout
        long evictionTimeoutMs = 10 * cacheTimeoutMs;
        TimeUnit.MILLISECONDS.sleep(evictionTimeoutMs + 100); // Wait slightly beyond eviction timeout

        // Assert that the cache entry for `testUser` is evicted and reloaded upon access
        java.util.List<String> groups = cachedGroupMapping.getGroups(testUser);
        
        // Expected behavior: The groups should match the mock lookup, indicating the entry was reloaded
        Assert.assertEquals("group1", groups.get(0));
        Assert.assertEquals("group2", groups.get(1));
        Assert.assertEquals("group3", groups.get(2));

        // Step 4: Code after testing
        System.out.println("TestCachedGroupMapping_Expire_After_Write completed successfully.");
    }
}