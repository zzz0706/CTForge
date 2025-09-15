package org.apache.hadoop.security;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class GroupsConfigurationCoverageTest {

  private Configuration conf;
  private Groups groups;
  private MockGroupMapping.MockGroupMappingProvider mockProvider;

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.set("hadoop.security.group.mapping",
        MockGroupMapping.MockGroupMappingProvider.class.getName());
    mockProvider = mock(MockGroupMapping.MockGroupMappingProvider.class);
    MockGroupMapping.MockGroupMappingProvider.setDelegate(mockProvider);
  }

  @After
  public void tearDown() {
    MockGroupMapping.MockGroupMappingProvider.reset();
  }

  @Test
  public void testCacheTimeoutConfigurationIsUsed() throws Exception {
    // 1. Configuration as input – use the real key so mutants on the key name are killed
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 1L);

    // 2. Prepare the test conditions.
    groups = new Groups(conf);

    when(mockProvider.getGroups("alice"))
        .thenReturn(Arrays.asList("groupA", "groupB"));

    // 3. Test code.
    List<String> firstCall = groups.getGroups("alice");
    verify(mockProvider, atLeastOnce()).getGroups("alice");

    // Sleep for more than the configured timeout (1 second) to ensure the cache would expire
    Thread.sleep(1100);

    // Second call should still return cached data because refreshAfterWrite has not been triggered
    List<String> secondCall = groups.getGroups("alice");

    // 4. Code after testing.
    assertEquals(Arrays.asList("groupA", "groupB"), firstCall);
    assertEquals(Arrays.asList("groupA", "groupB"), secondCall);
    // Cache did not hit the provider again
    verify(mockProvider, atLeastOnce()).getGroups("alice");
  }

  @Test
  public void testNegativeCacheConfigurationIsUsed() throws Exception {
    // 1. Configuration as input – use the real key so mutants on the key name are killed
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_NEGATIVE_CACHE_SECS, 60L);

    // 2. Prepare the test conditions.
    groups = new Groups(conf);

    when(mockProvider.getGroups("bob"))
        .thenThrow(new IOException("User not found"));

    // 3. Test code.
    try {
      groups.getGroups("bob");
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected
    }

    // Second call should use negative cache
    try {
      groups.getGroups("bob");
      fail("Expected IOException");
    } catch (IOException e) {
      // Expected
    }

    // 4. Code after testing.
    verify(mockProvider, atLeastOnce()).getGroups("bob");
  }

  @Test
  public void testRefreshInvalidatesCacheAndReusesConfiguration() throws Exception {
    // 1. Configuration as input – use the real key so mutants on the key name are killed
    conf.setLong(CommonConfigurationKeys.HADOOP_SECURITY_GROUPS_CACHE_SECS, 5L);

    // 2. Prepare the test conditions.
    groups = new Groups(conf);

    when(mockProvider.getGroups("charlie"))
        .thenReturn(Arrays.asList("group1", "group2"));

    // 3. Test code.
    List<String> firstCall = groups.getGroups("charlie");
    verify(mockProvider, atLeastOnce()).getGroups("charlie");

    // Change the underlying data
    when(mockProvider.getGroups("charlie"))
        .thenReturn(Arrays.asList("group3", "group4"));

    // Explicit refresh should invalidate cache regardless of timeout
    groups.refresh();

    List<String> secondCall = groups.getGroups("charlie");

    // 4. Code after testing.
    assertEquals(Arrays.asList("group1", "group2"), firstCall);
    assertEquals(Arrays.asList("group3", "group4"), secondCall);
    verify(mockProvider, atLeast(2)).getGroups("charlie");
  }

  /**
   * Inner static class that delegates to a Mockito mock so we can verify
   * interactions after the Groups instance is built.
   */
  public static class MockGroupMapping {
    public static class MockGroupMappingProvider implements GroupMappingServiceProvider {
      private static GroupMappingServiceProvider delegate;

      public static void setDelegate(GroupMappingServiceProvider mock) {
        delegate = mock;
      }

      public static void reset() {
        delegate = null;
      }

      @Override
      public List<String> getGroups(String user) throws IOException {
        return delegate.getGroups(user);
      }

      @Override
      public void cacheGroupsRefresh() throws IOException {
        delegate.cacheGroupsRefresh();
      }

      @Override
      public void cacheGroupsAdd(List<String> groups) throws IOException {
        delegate.cacheGroupsAdd(groups);
      }
    }
  }
}