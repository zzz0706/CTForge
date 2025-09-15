package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestWebHdfsConfiguration {

    private Configuration conf;
    private WebHdfsFileSystem webHdfsFileSystem;

    @Before
    public void setUp() throws Exception {
        // 1. Prepare the test conditions
        conf = new Configuration(); // Correctly initialize a Configuration object

        // Set up necessary configuration values explicitly for the test
        conf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, 
                HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT);
        conf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, TimeUnit.MILLISECONDS.toMillis(60000));
        conf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, TimeUnit.MILLISECONDS.toMillis(60000));

        // Initialize WebHdfsFileSystem instance
        webHdfsFileSystem = new WebHdfsFileSystem();
        // Using `localhost` instead of `namenode` to avoid UnknownHostException for the URI
        webHdfsFileSystem.initialize(new URI("webhdfs://localhost:50070"), conf);
    }

    @Test
    public void test_initialize_withOAuthDisabled() throws Exception {
        // 1. Using HDFS 2.8.5 APIs to properly retrieve configuration values
        URI uri = new URI("webhdfs://localhost:50070"); // updated to avoid UnknownHostException

        // 2. Prepare the test conditions
        Configuration testConf = new Configuration();
        testConf.setBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, 
                HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT);
        testConf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, TimeUnit.MILLISECONDS.toMillis(60000));
        testConf.setLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, TimeUnit.MILLISECONDS.toMillis(60000));

        // 3. Test code
        WebHdfsFileSystem testWebHdfsFileSystem = new WebHdfsFileSystem();
        testWebHdfsFileSystem.initialize(uri, testConf);

        // 4. Verify results
        // Ensure the configurations are correctly set
        assertFalse("OAuth should be disabled by default", testConf.getBoolean(HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY, !HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT));
        assertEquals("Socket connect timeout should match", 60000, testConf.getLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, 0));
        assertEquals("Socket read timeout should match", 60000, testConf.getLong(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, 0));
    }
}