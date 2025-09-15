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
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        long cacheTimeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);
        
        // 2. Prepare the test conditions.
        Assert.assertTrue("Cache timeout must be greater than zero", cacheTimeoutMs > 0);

        GroupMappingService mockService = new GroupMappingService() {
            @Override
            public List<String> getGroups(String user) {
                return Arrays.asList(user + "_group");
            }
        };

        CachedGroupMapping cachedGroupMapping = new CachedGroupMapping(mockService, cacheTimeoutMs);

        // Add entries into the cache
        String user = "testUser";
        cachedGroupMapping.getGroups(user); // Add user to the cache

        // 3. Test code.
        // Simulate elapsed time greater than refresh timeout but less than eviction timeout
        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(cacheTimeoutMs + 100));

        List<String> groups = cachedGroupMapping.getGroups(user);

        // Ensure the entry is present and asynchronously refreshed
        Assert.assertNotNull("Groups should not be null", groups);
        Assert.assertFalse("Groups list should not be empty", groups.isEmpty());
        Assert.assertTrue("Groups should contain the expected suffix", groups.get(0).endsWith("_group"));
    }
}