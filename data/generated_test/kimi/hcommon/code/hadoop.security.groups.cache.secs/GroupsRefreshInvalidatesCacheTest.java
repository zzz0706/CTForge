package org.apache.hadoop.security;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

public class GroupsRefreshInvalidatesCacheTest {

  @Test
  public void manualRefreshInvalidatesCacheRegardlessOfTimeout() throws Exception {
    // 1. Configuration as input
    Configuration conf = new Configuration();

    // 2. Prepare the test conditions.
    //    In Hadoop 2.8.5 the provider is obtained via the configuration key
    //    hadoop.security.group.mapping and must implement GroupMappingServiceProvider.
    //    We therefore stub the class name so that Groups loads a Mockito mock.
    conf.set("hadoop.security.group.mapping", MockGroupMapping.class.getName());

    // 3. Test code.
    Groups groups = new Groups(conf);

    // Mock the static method inside MockGroupMapping
    MockGroupMapping.mock = mock(GroupMappingServiceProvider.class);
    when(MockGroupMapping.mock.getGroups("bob"))
        .thenReturn(Arrays.asList("group1", "group2"));

    List<String> firstCall = groups.getGroups("bob");
    // verify provider hit once
    verify(MockGroupMapping.mock, times(1)).getGroups("bob");

    // Trigger refresh
    groups.refresh();

    List<String> secondCall = groups.getGroups("bob");

    // 4. Code after testing.
    assertEquals(Arrays.asList("group1", "group2"), firstCall);
    assertEquals(Arrays.asList("group1", "group2"), secondCall);
    // after refresh the provider should be consulted again
    verify(MockGroupMapping.mock, times(2)).getGroups("bob");
  }

  /**
   * Inner static class that delegates to a Mockito mock so we can verify
   * interactions after the Groups instance is built.
   */
  public static class MockGroupMapping implements GroupMappingServiceProvider {
    public static GroupMappingServiceProvider mock;

    @Override
    public List<String> getGroups(String user) throws IOException {
      return mock.getGroups(user);
    }

    @Override
    public void cacheGroupsRefresh() throws IOException {
      mock.cacheGroupsRefresh();
    }

    @Override
    public void cacheGroupsAdd(List<String> groups) throws IOException {
      mock.cacheGroupsAdd(groups);
    }
  }
}