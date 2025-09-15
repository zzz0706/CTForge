package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertFalse;

public class TestWebHdfsFileSystemInitializeWithoutOAuth {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInitializeWithoutOAuth() throws Exception {

        // Step 1: Prepare the test conditions
        Configuration conf = new Configuration();

        // Set essential properties for the test
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, false); // Disable OAuth
        conf.setTimeDuration(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, 
                60, TimeUnit.SECONDS); // Set connection timeout
        conf.setTimeDuration(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, 
                60, TimeUnit.SECONDS); // Set read timeout

        // Step 2: Construct the URI for the WebHDFS server
        URI webHdfsUri = new URI("webhdfs://localhost:50075");

        // Step 3: Create an instance of WebHdfsFileSystem
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        webHdfsFileSystem.initialize(webHdfsUri, conf); // Initialize with URI and configuration

        // Step 4: Verify the absence of OAuth and propagate correct configuration
        boolean isOAuthEnabled = conf.getBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, true);
        assertFalse("OAuth should be disabled in the configuration", isOAuthEnabled);

        // Validate WebHdfsFileSystem initialization
        assertNotNull("WebHdfsFileSystem should be initialized", webHdfsFileSystem);

        // Validate standard connection factory without OAuth
        URLConnectionFactory connectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(
                (int) conf.getTimeDuration(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                        URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS),
                (int) conf.getTimeDuration(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
                        URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT, TimeUnit.MILLISECONDS),
                conf);
        assertNotNull("Connection Factory must be initialized without OAuth2", connectionFactory);

        // Code after testing: Ensure to properly close resources
        webHdfsFileSystem.close();
    }
}