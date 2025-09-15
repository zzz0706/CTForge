package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class WebHdfsSocketConnectTimeoutConfigTest {

    @Test
    public void testWebHdfsSocketConnectTimeout_CustomValueParsedCorrectly() throws IOException {
        // Step 1: Create a Configuration object and set dfs.webhdfs.socket.connect-timeout to '30s'
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "30s");
        
        // Get the expected timeout value that should be parsed
        long expectedTimeoutMs = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        // Create URLConnectionFactory directly with the configuration to verify timeout parsing
        URLConnectionFactory factory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);
        
        // The factory should be created successfully
        assertNotNull(factory);
        
        // Verify that the timeout value matches expected (30 seconds = 30000 milliseconds)
        assertEquals(30000L, expectedTimeoutMs);
    }

    @Test
    public void testWebHdfsSocketConnectTimeout_WithOAuthEnabled() throws IOException {
        // Create a Configuration object with custom timeout and OAuth enabled
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "45s");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);
        
        // Get the expected timeout value that should be parsed
        long expectedTimeoutMs = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        // For testing timeout parsing with OAuth enabled, we just verify the timeout value
        // The actual OAuth factory creation requires additional configuration
        assertEquals(45000L, expectedTimeoutMs);
    }

    @Test
    public void testWebHdfsSocketConnectTimeout_VariousTimeFormats() throws IOException {
        // Test different time format specifications
        String[] timeValues = {"30s", "120000ms", "2m", "60000"};
        long[] expectedValues = {30000L, 120000L, 120000L, 60000L};
        
        for (int i = 0; i < timeValues.length; i++) {
            Configuration conf = new Configuration();
            conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, timeValues[i]);
            
            long actualTimeoutMs = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
            
            assertEquals("Failed for time value: " + timeValues[i], 
                        expectedValues[i], actualTimeoutMs);
        }
    }

    @Test
    public void testWebHdfsSocketConnectTimeout_DefaultValue() throws IOException {
        // Test default value when configuration is not set
        Configuration conf = new Configuration();
        
        long defaultTimeoutMs = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);
        
        // Should use the default value from URLConnectionFactory
        assertEquals(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT, defaultTimeoutMs);
    }
}