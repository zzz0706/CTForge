package alluxio.security.group;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CachedGroupMappingTest {

  @Test
  public void cacheEnabledWhenTimeoutPositive() throws Exception {
    // 1. Instantiate a fresh Configuration without overrides
    AlluxioConfiguration conf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Obtain the default timeout value from configuration
    long timeoutMs = conf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);

    // 3. Mock the underlying GroupMappingService
    GroupMappingService underlying = mock(GroupMappingService.class);
    List<String> bobGroups = Arrays.asList("admins", "users");
    when(underlying.getGroups("bob")).thenReturn(bobGroups);

    // 4. Create CachedGroupMapping using the timeout from configuration
    CachedGroupMapping cached = new CachedGroupMapping(underlying, timeoutMs);

    // 5. Invoke getGroups twice
    List<String> first = cached.getGroups("bob");
    List<String> second = cached.getGroups("bob");

    // 6. Verify the underlying service was called exactly once
    verify(underlying).getGroups("bob");

    // 7. Assert the returned lists are equal (cache hit)
    assertEquals(first, second);
  }
}