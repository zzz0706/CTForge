package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TestGroupsConfiguration {

    // Verify that refresh() handles IOException during cache refresh gracefully
    @Test
    public void test_refresh_handlesIOExceptionGracefully() throws IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300); // Set explicitly for test coverage
        long cacheTimeoutMillis = conf.getLong(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT
        ) * 1000;

        // Prepare the input conditions for unit testing
        Groups groups = spy(new Groups(conf));
        Cache<String, List<String>> mockCache = spy(
            CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheTimeoutMillis, TimeUnit.MILLISECONDS)
            .expireAfterWrite(10 * cacheTimeoutMillis, TimeUnit.MILLISECONDS)
            .build()
        );
        doReturn(mockCache).when(groups).getCache();

        // Mock the group provider's cacheGroupsRefresh() method to throw an IOException
        doThrow(new IOException("Simulated IOException"))
            .when(groups.getGroupMappingProvider())
            .cacheGroupsRefresh();

        // Invoke refresh() method
        groups.refresh();

        // Test correctness by verifying the cache invalidation and negative cache handling
        verify(mockCache, times(1)).invalidateAll();
        verify(groups, times(1)).isNegativeCacheEnabled();
        verify(groups.getNegativeCache(), times(1)).clear();
    }

    // Verify getGroups() retrieves groups from cache correctly for a given user
    @Test
    public void test_getGroups_retrievesFromCacheCorrectly() throws IOException, ExecutionException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300); // Set explicitly for test coverage
        long cacheTimeoutMillis = conf.getLong(
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS,
            CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS_DEFAULT
        ) * 1000;

        // Prepare the input conditions
        Groups groups = spy(new Groups(conf));
        Cache<String, List<String>> mockCache = spy(
            CacheBuilder.newBuilder()
            .refreshAfterWrite(cacheTimeoutMillis, TimeUnit.MILLISECONDS)
            .expireAfterWrite(10 * cacheTimeoutMillis, TimeUnit.MILLISECONDS)
            .build()
        );
        doReturn(mockCache).when(groups).getCache();

        // Simulate the cache containing the user's groups
        String user = "testUser";
        List<String> userGroups = Arrays.asList("group1", "group2");
        mockCache.put(user, userGroups);

        // Mock cache behavior to return the predefined userGroups
        when(mockCache.get(eq(user))).thenReturn(userGroups);

        // Invoke getGroups() method
        List<String> retrievedGroups = groups.getGroups(user);

        // Test correctness by verifying the retrieved groups and cache interactions
        verify(mockCache, times(1)).get(user);
        assert retrievedGroups.equals(userGroups);
    }

    // Verify getGroups() throws IOException when user is in the negative cache
    @Test(expected = IOException.class)
    public void test_getGroups_throwsIOExceptionOnNegativeCacheHit() throws IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
        conf.setLong(CommonConfigurationKeysPublic.HADOOP_SECURITY_GROUPS_CACHE_SECS, 300); // Set explicitly for test coverage
        Groups groups = spy(new Groups(conf));

        // Mock negative cache behavior
        doReturn(true).when(groups).isNegativeCacheEnabled();
        doReturn(true).when(groups.getNegativeCache()).contains("testUser");

        // Invoke getGroups() method with a user in the negative cache
        groups.getGroups("testUser");
        
        // Exception is expected, correctness is confirmed by the @Test(expected) annotation
    }
}