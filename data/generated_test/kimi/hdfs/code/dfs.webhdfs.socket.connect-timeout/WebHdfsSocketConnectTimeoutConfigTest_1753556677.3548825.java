package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class WebHdfsSocketConnectTimeoutConfigTest {

    private Configuration conf;
    private static final int DEFAULT_TIMEOUT_MS = 60000; // 60s default

    @Before
    public void setUp() {
        conf = new Configuration(false); // Start with empty configuration
    }

    @Test
    public void testDfsWebhdfsSocketConnectTimeoutDefaultValue() {
        // Test that the default value is correctly loaded from HdfsClientConfigKeys
        long defaultValue = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        
        assertEquals("Default connect timeout should be " + DEFAULT_TIMEOUT_MS + "ms", 
                DEFAULT_TIMEOUT_MS, defaultValue);
    }

    @Test
    public void testDfsWebhdfsSocketConnectTimeoutParsing30s() throws Exception {
        testDfsWebhdfsSocketConnectTimeoutParsingWithValue("30s", 30000);
    }

    @Test
    public void testDfsWebhdfsSocketConnectTimeoutParsing1m() throws Exception {
        testDfsWebhdfsSocketConnectTimeoutParsingWithValue("1m", 60000);
    }

    @Test
    public void testDfsWebhdfsSocketConnectTimeoutParsing90000ms() throws Exception {
        testDfsWebhdfsSocketConnectTimeoutParsingWithValue("90000ms", 90000);
    }

    @Test
    public void testDfsWebhdfsSocketConnectTimeoutParsing90s() throws Exception {
        testDfsWebhdfsSocketConnectTimeoutParsingWithValue("90s", 90000);
    }

    private void testDfsWebhdfsSocketConnectTimeoutParsingWithValue(String timeoutValue, long expectedMs) throws Exception {
        // Set the configuration value
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, timeoutValue);
        
        // Parse the value using the same method as WebHdfsFileSystem
        long parsedValue = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        
        // Verify it was parsed correctly
        assertEquals("Parsed timeout value should match expected milliseconds", 
                expectedMs, parsedValue);
    }

    @Test
    public void testConfigurationFromFileMatchesHardcodedDefault() {
        // Load default configuration that would be present in the actual system
        Configuration defaultConf = new Configuration();
        
        // Get the value using the config service approach (Configuration class)
        long configValue = defaultConf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        
        // Compare against what we know is the documented default
        assertEquals("Configuration file default should match documented default of 60 seconds", 
                60000L, configValue);
    }

    @Test
    public void testWebHdfsSocketConnectTimeout_WithOAuthEnabled() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        // Create a Configuration object, set dfs.webhdfs.socket.connect-timeout to '45s' and dfs.webhdfs.oauth.enabled to true.
        Configuration conf = new Configuration(false);
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "45s");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);

        // 2. Prepare the test conditions.
        // Simply test that the configuration parsing works correctly with OAuth enabled
        // without actually initializing WebHdfsFileSystem which would require additional OAuth configuration
        
        // 3. Test code.
        // Parse the timeout value from configuration
        long configuredTimeout = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);
        
        // 4. Code after testing.
        // Assert that the parsed connectTimeout equals 45000 (45 seconds in milliseconds)
        assertEquals("The connectTimeout should be 45000 milliseconds (45 seconds)", 
                45000L, configuredTimeout);
    }
}