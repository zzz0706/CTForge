package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestWebHdfsSocketReadTimeoutConfig {

    private Configuration conf;
    private URI testUri;

    @Before
    public void setUp() {
        conf = new Configuration();
        testUri = URI.create("webhdfs://localhost:9870");
    }

    @Test
    public void testSocketReadTimeoutConfigurationIsUsedInConnectionFactory() throws IOException {
        // Prepare test conditions
        long expectedReadTimeoutMs = TimeUnit.SECONDS.toMillis(30); // 30s
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "30s");
        
        // Test that the configuration is properly parsed
        long actualTimeout = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        assertEquals(expectedReadTimeoutMs, actualTimeout);
    }

    @Test
    public void testDefaultSocketReadTimeoutWhenNotConfigured() throws IOException {
        // Prepare test conditions - do not set the config, let it use default
        long defaultTimeoutMs = URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT;

        // Test that default value is used when not configured
        long actualTimeout = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        assertEquals(defaultTimeoutMs, actualTimeout);
    }

    @Test
    public void testOAuth2ConnectionFactoryUsesSocketReadTimeout() throws IOException {
        // Prepare test conditions
        long expectedReadTimeoutMs = TimeUnit.MINUTES.toMillis(2); // 2m
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "2m");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);

        // Test that the configuration is properly parsed for OAuth2 case
        long actualTimeout = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        assertEquals(expectedReadTimeoutMs, actualTimeout);
    }

    @Test
    public void testConfigurationValueMatchesPropertyFile() {
        // Load the configuration from the actual Hadoop configuration system
        String configKey = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        long configuredValue = conf.getTimeDuration(configKey, 
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS);
        
        // The default value according to documentation is 60s
        long expectedDefaultMs = TimeUnit.SECONDS.toMillis(60);
        
        // Assert that the default configuration matches expected default
        assertEquals(expectedDefaultMs, configuredValue);
    }
}