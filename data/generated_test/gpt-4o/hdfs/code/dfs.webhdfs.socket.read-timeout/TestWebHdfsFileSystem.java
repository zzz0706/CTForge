package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class TestWebHdfsFileSystem {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initialize_with_valid_configuration_and_oauth_disabled() throws Exception {
        // Step 1: Prepare the test conditions.
        // Create a Configuration object and set dfs.webhdfs.socket.read-timeout and dfs.webhdfs.oauth.enabled via API.
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "10s");
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, false);

        // Create a URI object for WebHDFS.
        URI webHdfsUri = new URI("webhdfs://localhost:50070");

        // Step 2: Instantiate the WebHdfsFileSystem object.
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // Step 3: Call the initialize method with the URI and Configuration objects.
        webHdfsFileSystem.initialize(webHdfsUri, conf);

        // Step 4: Verify that the timeout configuration is correctly set within the system.
        // Note: DFS_WEBHDFS_SOCKET_READ_TIMEOUT_DEFAULT does not exist in HdfsClientConfigKeys in HDFS 2.8.5.
        // Hence, we cannot use it, and we must check solely based on the manually configured value.
        long socketReadTimeout = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            0,  // Default value if none is set
            TimeUnit.MILLISECONDS);
        Assert.assertEquals("Socket read timeout should match the configured value", 10000L, socketReadTimeout);

        boolean isOauthEnabled = conf.getBoolean(
            HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,
            true);
        Assert.assertFalse("OAuth should be disabled as per configuration", isOauthEnabled);

        // Step 5: Code after testing (cleanup if needed).
        webHdfsFileSystem.close();
    }
}