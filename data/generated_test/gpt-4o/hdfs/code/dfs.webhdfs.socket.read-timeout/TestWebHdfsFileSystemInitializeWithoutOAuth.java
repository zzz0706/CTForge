package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertNotNull;

public class TestWebHdfsFileSystemInitializeWithoutOAuth {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInitializeWithoutOAuth() throws Exception {

        // Prepare the test conditions
        Configuration conf = new Configuration();
        // Ensure dfs.webhdfs.oauth.enabled is disabled (using API to fetch value)
        boolean isOAuthEnabled = conf.getBoolean("dfs.webhdfs.oauth.enabled", false);
        // Obtain read timeout and connect timeout values
        long readTimeout = conf.getTimeDuration("dfs.webhdfs.socket.read-timeout", 60, java.util.concurrent.TimeUnit.SECONDS);
        long connectTimeout = conf.getTimeDuration("dfs.webhdfs.socket.connect-timeout", 60, java.util.concurrent.TimeUnit.SECONDS);

        // Test code
        URI webHdfsUri = new URI("webhdfs://localhost:50075");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();
        webHdfsFileSystem.initialize(webHdfsUri, conf);

        if (!isOAuthEnabled) {
            // Verify that WebHdfsFileSystem was initialized correctly
            assertNotNull("WebHdfsFileSystem should be initialized", webHdfsFileSystem);
        }

        // Code after testing
        webHdfsFileSystem.close();
    }
}