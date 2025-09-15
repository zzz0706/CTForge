package alluxio.security.group;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.security.group.GroupMappingService;
import alluxio.security.group.CachedGroupMapping;
import com.google.common.collect.ImmutableList;
import org.mockito.Mockito;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

public class CachedGroupMappingTest {

    @Test
    public void testCachedGroupMappingExpireAfterWrite() throws Exception {
        // 1. Obtain a valid configuration instance using Alluxio InstancedConfiguration.
        AlluxioConfiguration configuration = InstancedConfiguration.defaults();
        // Retrieve the timeout value for group mapping cache.
        long timeoutMs = configuration.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

        // Test prerequisite: Ensure the timeout value is greater than zero to enable caching.
        Assert.assertTrue("Timeout should be non-zero for cache to be enabled", timeoutMs > 0);

        // 2. Prepare a mocked GroupMappingService.
        GroupMappingService mockService = Mockito.mock(GroupMappingService.class);

        // Mock the behavior of the service's getGroups(user) method.
        Mockito.when(mockService.getGroups(Mockito.anyString()))
               .thenReturn(ImmutableList.of("mockGroup")); // Changed to ImmutableList for proper implementation.

        // 3. Create a CachedGroupMapping instance using the configuration and the mocked service.
        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(mockService, timeoutMs);

        // Populate the cache with an initial call to getGroups(user).
        String testUser = "testUser";
        cachedGroupMapping.getGroups(testUser);

        // Verify that the initial call fetched data from the underlying service.
        Mockito.verify(mockService, Mockito.times(1)).getGroups(testUser);

        // 4. Wait for a duration exceeding the cache timeout to ensure cache eviction.
        long evictionDurationMs = 10 * timeoutMs;
        TimeUnit.MILLISECONDS.sleep(evictionDurationMs + 1000); // Adding buffer to account for timing variances.

        // 5. Make another call to getGroups(user) after the eviction duration.
        cachedGroupMapping.getGroups(testUser);

        // Verify that the cache has been invalidated, and another call was made to the underlying service.
        Mockito.verify(mockService, Mockito.times(2)).getGroups(testUser);
    }
}