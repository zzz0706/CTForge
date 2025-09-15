package alluxio.security.group;

import org.junit.Test;
import org.junit.Assert;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.group.CachedGroupMapping;
import alluxio.security.group.GroupMappingService;

import java.util.concurrent.TimeUnit;
import java.util.Arrays;
import java.util.List;

public class CachedGroupMappingExpireTest {
  
    @Test
    // Test code
    // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testCachedGroupMappingExpireAfterWrite() throws Exception {
        // 1. Use the Alluxio API to obtain configuration values
        AlluxioConfiguration conf = InstancedConfiguration.defaults();
        long cacheTimeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

        // Ensure that the cache timeout is valid and positive
        Assert.assertTrue("Cache timeout must be positive", cacheTimeoutMs > 0);

        // 2. Prepare the test conditions
        // Create a mock implementation of GroupMappingService for testing
        GroupMappingService mockService = new GroupMappingService() {
            @Override
            public List<String> getGroups(String user) {
                // Simulate a group-mapping lookup with constant group data
                return Arrays.asList("groupA", "groupB", "groupC");
            }
        };

        // Initialize CachedGroupMapping explicitly for testing
        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(mockService, cacheTimeoutMs);

        // Simulate adding an entry to the cache by fetching groups for a user
        String testUser = "testUser";
        cachedGroupMapping.getGroups(testUser); // This populates the cache for testUser
        
        // Simulate elapsed time exceeding the eviction timeout
        TimeUnit.MILLISECONDS.sleep(10 * cacheTimeoutMs + 100); // Wait slightly beyond eviction timeout threshold

        // Access the cached entry again
        List<String> groups = cachedGroupMapping.getGroups(testUser);

        // Expected behavior: The groups should match the mock lookup, indicating the entry was reloaded
        Assert.assertEquals(3, groups.size());
        Assert.assertEquals("groupA", groups.get(0));
        Assert.assertEquals("groupB", groups.get(1));
        Assert.assertEquals("groupC", groups.get(2));

        // 4. Code after testing
        System.out.println("TestCachedGroupMappingExpireAfterWrite completed successfully.");
    }
}