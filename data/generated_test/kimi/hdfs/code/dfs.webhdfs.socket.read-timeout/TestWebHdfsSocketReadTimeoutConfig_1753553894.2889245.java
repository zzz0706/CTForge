package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@PrepareForTest({URLConnectionFactory.class, UserGroupInformation.class})
@RunWith(PowerMockRunner.class)
public class TestWebHdfsSocketReadTimeoutConfig {

    private Configuration conf;
    private Properties configProps;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration(false);
        // Load default configuration values from the configuration file
        configProps = new Properties();
        configProps.load(this.getClass().getClassLoader()
                .getResourceAsStream("core-default.xml"));
        configProps.load(this.getClass().getClassLoader()
                .getResourceAsStream("hdfs-default.xml"));
    }

    @Test
    public void testWebHdfsSocketReadTimeoutDefaultValue() throws IOException {
        // Prepare test conditions - no explicit setting of the config value
        String key = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        long defaultValue = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);

        // Reference loader comparison
        String expectedValueFromProps = configProps.getProperty(key, "60s");
        long expectedValueParsed = parseTimeDuration(expectedValueFromProps, URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT);

        assertEquals("Default read timeout should match configuration file value",
                expectedValueParsed, defaultValue);
    }

    @Test
    public void testWebHdfsSocketReadTimeoutCustomValue() throws IOException {
        // Prepare test conditions - set custom configuration value
        String key = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        conf.set(key, "30s");

        long configuredValue = conf.getTimeDuration(key,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);

        // Reference loader comparison
        long expectedValue = parseTimeDuration("30s", URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT);

        assertEquals("Configured read timeout should match expected value",
                expectedValue, configuredValue);
    }

    @Test
    public void testWebHdfsSocketReadTimeoutPassedToConnectionFactory() throws IOException {
        // Prepare test conditions
        String key = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        conf.set(key, "45s");
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, "false");

        // Mock static methods to capture arguments
        PowerMockito.mockStatic(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = mock(URLConnectionFactory.class);
        PowerMockito.when(URLConnectionFactory.newDefaultURLConnectionFactory(
                anyInt(), anyInt(), any(Configuration.class))).thenReturn(mockFactory);

        // Mock UserGroupInformation to avoid security issues
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(UserGroupInformation.getCurrentUser()).thenReturn(mockUgi);
        when(mockUgi.getUserName()).thenReturn("testuser");

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        webHdfsFileSystem.initialize(URI.create("webhdfs://localhost:9870"), conf);

        // Capture arguments passed to URLConnectionFactory
        ArgumentCaptor<Integer> connectTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> readTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Configuration> confCaptor = ArgumentCaptor.forClass(Configuration.class);

        PowerMockito.verifyStatic();
        URLConnectionFactory.newDefaultURLConnectionFactory(
                connectTimeoutCaptor.capture(), readTimeoutCaptor.capture(), confCaptor.capture());

        // Verify the read timeout value passed to the factory
        long expectedReadTimeout = conf.getTimeDuration(key,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);

        assertEquals("Read timeout passed to connection factory should match configured value",
                (int) expectedReadTimeout, readTimeoutCaptor.getValue().intValue());
    }

    @Test
    public void testWebHdfsSocketReadTimeoutWithOAuthEnabled() throws IOException {
        // Prepare test conditions
        String key = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        String oauthKey = HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY;
        conf.set(key, "120s");
        conf.set(oauthKey, "true");

        // Mock static methods to capture arguments
        PowerMockito.mockStatic(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = mock(URLConnectionFactory.class);
        PowerMockito.when(URLConnectionFactory.newOAuth2URLConnectionFactory(
                anyInt(), anyInt(), any(Configuration.class))).thenReturn(mockFactory);

        // Mock UserGroupInformation to avoid security issues
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation mockUgi = mock(UserGroupInformation.class);
        when(UserGroupInformation.getCurrentUser()).thenReturn(mockUgi);
        when(mockUgi.getUserName()).thenReturn("testuser");

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        webHdfsFileSystem.initialize(URI.create("webhdfs://localhost:9870"), conf);

        // Capture arguments passed to URLConnectionFactory
        ArgumentCaptor<Integer> connectTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> readTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Configuration> confCaptor = ArgumentCaptor.forClass(Configuration.class);

        PowerMockito.verifyStatic();
        URLConnectionFactory.newOAuth2URLConnectionFactory(
                connectTimeoutCaptor.capture(), readTimeoutCaptor.capture(), confCaptor.capture());

        // Verify the read timeout value passed to the factory
        long expectedReadTimeout = conf.getTimeDuration(key,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);

        assertEquals("Read timeout passed to OAuth2 connection factory should match configured value",
                (int) expectedReadTimeout, readTimeoutCaptor.getValue().intValue());
    }

    /**
     * Parse time duration string to milliseconds
     * @param value time duration string like "60s", "30m", "1h"
     * @param defaultValue default value in milliseconds
     * @return parsed time duration in milliseconds
     */
    private long parseTimeDuration(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        
        try {
            if (value.endsWith("ms")) {
                return Long.parseLong(value.substring(0, value.length() - 2));
            } else if (value.endsWith("s")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 1000;
            } else if (value.endsWith("m")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 60 * 1000;
            } else if (value.endsWith("h")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 60 * 60 * 1000;
            } else if (value.endsWith("d")) {
                return Long.parseLong(value.substring(0, value.length() - 1)) * 24 * 60 * 60 * 1000;
            } else {
                // Assume it's in seconds if no unit specified
                return Long.parseLong(value) * 1000;
            }
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}