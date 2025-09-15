package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.test.GenericTestUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.powermock.api.mockito.PowerMockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({WebHdfsFileSystem.class, URLConnectionFactory.class, UserGroupInformation.class})
public class TestWebHdfsSocketReadTimeoutConfig {

    private Configuration conf;
    private URI testUri;

    @Before
    public void setUp() {
        conf = new Configuration();
        testUri = URI.create("webhdfs://localhost:9870");
        
        // Mock UserGroupInformation to avoid security-related issues in tests
        PowerMockito.mockStatic(UserGroupInformation.class);
        UserGroupInformation mockUgi = org.mockito.Mockito.mock(UserGroupInformation.class);
        try {
            org.mockito.Mockito.when(UserGroupInformation.getCurrentUser()).thenReturn(mockUgi);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    // testWebHdfsReadTimeoutPropagatedToConnectionFactory
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testWebHdfsReadTimeoutPropagatedToConnectionFactory() throws Exception {
        // Prepare test conditions
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "30s");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, false);
        
        // Mock URLConnectionFactory to capture arguments
        PowerMockito.spy(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = org.mockito.Mockito.mock(URLConnectionFactory.class);
        PowerMockito.doReturn(mockFactory)
            .when(URLConnectionFactory.class, "newDefaultURLConnectionFactory", anyInt(), anyInt(), any(Configuration.class));

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webhdfs = new WebHdfsFileSystem();
        webhdfs.initialize(testUri, conf);

        // Verify that newDefaultURLConnectionFactory was called with correct readTimeout
        long expectedReadTimeoutMs = TimeUnit.SECONDS.toMillis(30); // 30s
        PowerMockito.verifyStatic(org.mockito.Mockito.times(1));
        URLConnectionFactory.newDefaultURLConnectionFactory(anyInt(), eq((int)expectedReadTimeoutMs), any(Configuration.class));
    }

    @Test
    // testOAuth2WebHdfsReadTimeoutPropagatedToConnectionFactory
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testOAuth2WebHdfsReadTimeoutPropagatedToConnectionFactory() throws Exception {
        // Prepare test conditions
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "45s");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);
        
        // Mock URLConnectionFactory to capture arguments
        PowerMockito.spy(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = org.mockito.Mockito.mock(URLConnectionFactory.class);
        PowerMockito.doReturn(mockFactory)
            .when(URLConnectionFactory.class, "newOAuth2URLConnectionFactory", anyInt(), anyInt(), any(Configuration.class));

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webhdfs = new WebHdfsFileSystem();
        webhdfs.initialize(testUri, conf);

        // Verify that newOAuth2URLConnectionFactory was called with correct readTimeout
        long expectedReadTimeoutMs = TimeUnit.SECONDS.toMillis(45); // 45s
        PowerMockito.verifyStatic(org.mockito.Mockito.times(1));
        URLConnectionFactory.newOAuth2URLConnectionFactory(anyInt(), eq((int)expectedReadTimeoutMs), any(Configuration.class));
    }

    @Test
    // testDefaultSocketReadTimeoutWhenNotConfigured
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDefaultSocketReadTimeoutWhenNotConfigured() throws Exception {
        // Prepare test conditions - do not set the config, let it use default
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, false);
        
        // Mock URLConnectionFactory to capture arguments
        PowerMockito.spy(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = org.mockito.Mockito.mock(URLConnectionFactory.class);
        PowerMockito.doReturn(mockFactory)
            .when(URLConnectionFactory.class, "newDefaultURLConnectionFactory", anyInt(), anyInt(), any(Configuration.class));

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webhdfs = new WebHdfsFileSystem();
        webhdfs.initialize(testUri, conf);

        // Verify that newDefaultURLConnectionFactory was called with default readTimeout
        PowerMockito.verifyStatic(org.mockito.Mockito.times(1));
        URLConnectionFactory.newDefaultURLConnectionFactory(anyInt(), eq(URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT), any(Configuration.class));
    }

    @Test
    // testSocketReadTimeoutWithDifferentTimeUnits
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSocketReadTimeoutWithDifferentTimeUnits() throws Exception {
        // Prepare test conditions with different time units
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "2m");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, false);
        
        // Mock URLConnectionFactory to capture arguments
        PowerMockito.spy(URLConnectionFactory.class);
        URLConnectionFactory mockFactory = org.mockito.Mockito.mock(URLConnectionFactory.class);
        PowerMockito.doReturn(mockFactory)
            .when(URLConnectionFactory.class, "newDefaultURLConnectionFactory", anyInt(), anyInt(), any(Configuration.class));

        // Initialize WebHdfsFileSystem
        WebHdfsFileSystem webhdfs = new WebHdfsFileSystem();
        webhdfs.initialize(testUri, conf);

        // Verify that newDefaultURLConnectionFactory was called with correct readTimeout (2 minutes = 120000 ms)
        long expectedReadTimeoutMs = TimeUnit.MINUTES.toMillis(2);
        PowerMockito.verifyStatic(org.mockito.Mockito.times(1));
        URLConnectionFactory.newDefaultURLConnectionFactory(anyInt(), eq((int)expectedReadTimeoutMs), any(Configuration.class));
    }
}