package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

@RunWith(PowerMockRunner.class)
@PrepareForTest({URLConnectionFactory.class, UserGroupInformation.class})
public class WebHdfsFileSystemConfigTest {

    private Configuration conf;
    private WebHdfsFileSystem webHdfsFileSystem;

    @Before
    public void setUp() throws IOException {
        PowerMockito.mockStatic(URLConnectionFactory.class);
        PowerMockito.mockStatic(UserGroupInformation.class);
        
        // Mock UserGroupInformation to avoid authentication issues
        UserGroupInformation mockUgi = PowerMockito.mock(UserGroupInformation.class);
        PowerMockito.when(UserGroupInformation.getCurrentUser()).thenReturn(mockUgi);
        PowerMockito.when(UserGroupInformation.isSecurityEnabled()).thenReturn(false);
        
        conf = new Configuration();
        webHdfsFileSystem = new WebHdfsFileSystem();
    }

    @Test
    public void testDfsWebhdfsSocketReadTimeoutIsUsedInConnectionFactory() throws IOException {
        // Given: Set the configuration value for read timeout
        long expectedReadTimeoutMs = 30000L; // 30 seconds
        conf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, expectedReadTimeoutMs);
        
        // Mock the URLConnectionFactory methods
        URLConnectionFactory mockFactory = PowerMockito.mock(URLConnectionFactory.class);
        PowerMockito.when(URLConnectionFactory.newDefaultURLConnectionFactory(
            any(Integer.class), any(Integer.class), any(Configuration.class))).thenReturn(mockFactory);

        // When: Initialize WebHdfsFileSystem with the configuration
        webHdfsFileSystem.initialize(java.net.URI.create("webhdfs://localhost:9870"), conf);

        // Then: Verify that URLConnectionFactory is called with correct read timeout
        verifyStatic(times(1));
        URLConnectionFactory.newDefaultURLConnectionFactory(
            any(Integer.class),
            eq((int) expectedReadTimeoutMs),
            any(Configuration.class)
        );
    }

    @Test
    public void testDfsWebhdfsSocketReadTimeoutUsesDefaultWhenNotSet() throws IOException {
        // Given: Do not set the configuration value, so default should be used
        long defaultReadTimeoutMs = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS
        );
        
        // Mock the URLConnectionFactory methods
        URLConnectionFactory mockFactory = PowerMockito.mock(URLConnectionFactory.class);
        PowerMockito.when(URLConnectionFactory.newDefaultURLConnectionFactory(
            any(Integer.class), any(Integer.class), any(Configuration.class))).thenReturn(mockFactory);

        // When: Initialize WebHdfsFileSystem with the configuration
        webHdfsFileSystem.initialize(java.net.URI.create("webhdfs://localhost:9870"), conf);

        // Then: Verify that URLConnectionFactory is called with default read timeout
        verifyStatic(times(1));
        URLConnectionFactory.newDefaultURLConnectionFactory(
            any(Integer.class),
            eq((int) defaultReadTimeoutMs),
            any(Configuration.class)
        );
    }

    @Test
    public void testDfsWebhdfsSocketReadTimeoutWithOAuthEnabled() throws IOException {
        // Given: Enable OAuth and set custom read timeout
        long expectedReadTimeoutMs = 45000L; // 45 seconds
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);
        conf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, expectedReadTimeoutMs);
        
        // Mock the URLConnectionFactory methods
        URLConnectionFactory mockFactory = PowerMockito.mock(URLConnectionFactory.class);
        PowerMockito.when(URLConnectionFactory.newOAuth2URLConnectionFactory(
            any(Integer.class), any(Integer.class), any(Configuration.class))).thenReturn(mockFactory);

        // When: Initialize WebHdfsFileSystem with the configuration
        webHdfsFileSystem.initialize(java.net.URI.create("webhdfs://localhost:9870"), conf);

        // Then: Verify that OAuth2 URLConnectionFactory is called with correct read timeout
        verifyStatic(times(1));
        URLConnectionFactory.newOAuth2URLConnectionFactory(
            any(Integer.class),
            eq((int) expectedReadTimeoutMs),
            any(Configuration.class)
        );
    }
}