package alluxio.security.group;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class CachedGroupMappingTest {

  private GroupMappingService mUnderlying;
  private AlluxioConfiguration mConf;

  @Before
  public void before() throws Exception {
    // Reset the singleton inside GroupMappingService.Factory
    java.lang.reflect.Field field =
        GroupMappingService.Factory.class.getDeclaredField("sCachedGroupMapping");
    field.setAccessible(true);
    field.set(null, null);
  }

  @After
  public void after() throws Exception {
    // Reset the singleton again to avoid side-effects for other tests
    java.lang.reflect.Field field =
        GroupMappingService.Factory.class.getDeclaredField("sCachedGroupMapping");
    field.setAccessible(true);
    field.set(null, null);
  }

  @Test
  public void cacheEnabledWhenTimeoutPositive() throws Exception {
    // 1. Instantiate a fresh Configuration without overrides
    mConf = new InstancedConfiguration(ConfigurationUtils.defaults());

    // 2. Verify the default timeout is positive (enabling cache)
    long timeoutMs = mConf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS);
    assert timeoutMs > 0 : "Default timeout must be positive for cache to be enabled";

    // 3. Prepare a mock underlying service
    mUnderlying = mock(GroupMappingService.class);
    List<String> bobGroups = Arrays.asList("admins", "users");
    when(mUnderlying.getGroups("bob")).thenReturn(bobGroups);

    // 4. Replace the underlying service that Factory creates
    // We do this by injecting a mock via reflection on the class name
    AlluxioConfiguration spyConf = mock(AlluxioConfiguration.class);
    when(spyConf.getClass(PropertyKey.SECURITY_GROUP_MAPPING_CLASS))
        .thenReturn((Class) mUnderlying.getClass());
    when(spyConf.getMs(PropertyKey.SECURITY_GROUP_MAPPING_CACHE_TIMEOUT_MS))
        .thenReturn(timeoutMs);

    // However, reflection-based instantiation in Factory will try to create a new instance,
    // so we instead use a partial mock/spy approach or use a dedicated test impl.
    // Simpler: directly invoke the CachedGroupMapping constructor as the test already did,
    // but still verify the Factory path by calling GroupMappingService.get(conf).

    // 5. Obtain the cached mapping via the Factory
    GroupMappingService.Factory.get(mConf);

    // 6. Get the cached service instance
    GroupMappingService cachedService = GroupMappingService.Factory.get(mConf);

    // 7. Cast to CachedGroupMapping to inject our mock
    CachedGroupMapping cached = (CachedGroupMapping) cachedService;
    // Replace underlying service via reflection
    java.lang.reflect.Field underlyingField = CachedGroupMapping.class.getDeclaredField("mService");
    underlyingField.setAccessible(true);
    underlyingField.set(cached, mUnderlying);

    // 8. Invoke getGroups twice
    List<String> first = cachedService.getGroups("bob");
    List<String> second = cachedService.getGroups("bob");

    // 9. Verify underlying service was called exactly once
    verify(mUnderlying, times(1)).getGroups("bob");

    // 10. Assert the returned lists are equal (cache hit)
    assertEquals(first, second);
  }
}