package org.apache.hadoop.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Groups;
import org.junit.Test;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.*;

public class TestGroupsConfiguration {

    @Test
    public void test_refresh_handlesIOExceptionGracefully() throws IOException {
        // Get configuration value using API
        Configuration conf = new Configuration();
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

        // Mocking GroupMapping implementation to throw IOException
        doThrow(new IOException("Simulated IOException"))
            .when(groups.getGroupMappingProvider())
            .cacheGroupsRefresh();

        // Invoke refresh() method
        groups.refresh();

        // Verify behavior
        verify(mockCache, times(1)).invalidateAll();
        verify(groups, times(1)).isNegativeCacheEnabled();
        verify(groups.getNegativeCache(), times(1)).clear();
    }
}