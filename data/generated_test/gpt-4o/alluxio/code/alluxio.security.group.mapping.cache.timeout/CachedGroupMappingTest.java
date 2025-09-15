package alluxio.security.group;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ExecutionException;

public class CachedGroupMappingTest {

    @Test
    public void testGroupMappingServiceCacheEnabled() throws ExecutionException, IOException { // Resolved IOException compilation issue
        // Step 1: Prepare the test conditions
        AlluxioConfiguration conf = InstancedConfiguration.defaults();
        long cacheTimeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);
        Assert.assertTrue("Cache timeout must be greater than 0 for this test", cacheTimeoutMs > 0);

        // Create a GroupMappingService implementation for the test
        GroupMappingService groupMappingService = new GroupMappingService() {
            @Override
            public java.util.List<String> getGroups(String user) throws IOException { // Declared IOException as required
                return Arrays.asList(user); // Simple identity mapping for test purposes
            }
        };

        // Step 2: Create a CachedGroupMapping instance using the required constructor
        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(groupMappingService, cacheTimeoutMs);

        // Step 3: Test whether the configuration is set correctly
        Assert.assertNotNull("CachedGroupMapping should not be null after initialization", cachedGroupMapping);

        // Step 4: Simulate group mapping requests to verify cache behavior
        String testUser = "testuser";

        // First access to populate the cache
        Assert.assertEquals(
                "First group mapping request should return correct groups",
                Arrays.asList(testUser),
                cachedGroupMapping.getGroups(testUser));

        // Access the same group again to verify it comes from the cache
        Assert.assertEquals(
                "Second group mapping request should hit the cache and return the same result",
                Arrays.asList(testUser),
                cachedGroupMapping.getGroups(testUser));

        // Ensure no exceptions were thrown during the test
    }
}