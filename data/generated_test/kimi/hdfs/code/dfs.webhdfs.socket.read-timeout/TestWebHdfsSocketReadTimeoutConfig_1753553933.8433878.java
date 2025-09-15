package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@PrepareForTest({URLConnectionFactory.class, UserGroupInformation.class})
@RunWith(PowerMockRunner.class)
public class TestWebHdfsSocketReadTimeoutConfig {

    private Configuration conf;

    @Before
    public void setUp() throws Exception {
        conf = new Configuration(false);
    }

    @Test
    // Test to verify that the dfs.webhdfs.socket.read-timeout configuration value is correctly used when OAuth2 is enabled
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testWebHdfsReadTimeoutWithOAuthEnabled() throws IOException {
        // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values
        String readTimeoutKey = HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY;
        String oauthEnabledKey = HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY;
        
        // 2. Prepare the test conditions
        conf.set(readTimeoutKey, "45s"); // Set read timeout to 45 seconds
        conf.setBoolean(oauthEnabledKey, true); // Enable OAuth2
        
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

        // 3. Test code
        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        webHdfsFileSystem.initialize(URI.create("webhdfs://localhost:9870"), conf);

        // Capture arguments passed to URLConnectionFactory
        ArgumentCaptor<Integer> connectTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> readTimeoutCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Configuration> confCaptor = ArgumentCaptor.forClass(Configuration.class);

        // Verify static method was called
        PowerMockito.verifyStatic(Mockito.times(1));
        URLConnectionFactory.newOAuth2URLConnectionFactory(
                connectTimeoutCaptor.capture(), readTimeoutCaptor.capture(), confCaptor.capture());

        // 4. Code after testing
        // Verify the read timeout value passed to the factory
        long expectedReadTimeout = conf.getTimeDuration(readTimeoutKey,
                URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
                TimeUnit.MILLISECONDS);

        assertEquals("Read timeout passed to OAuth2 connection factory should match configured value",
                (int) expectedReadTimeout, readTimeoutCaptor.getValue().intValue());
    }
}