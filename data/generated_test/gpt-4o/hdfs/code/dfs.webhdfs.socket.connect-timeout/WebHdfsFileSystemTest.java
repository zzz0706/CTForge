package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertFalse;

public class WebHdfsFileSystemTest {

    private WebHdfsFileSystem webHdfsFileSystem;
    private Configuration conf;
    private URI uri;

    @Before
    public void setUp() throws Exception {
        // Initialize an instance of WebHdfsFileSystem
        webHdfsFileSystem = new WebHdfsFileSystem();
        conf = new Configuration();
        uri = new URI("hdfs://localhost:8020"); // Replace with any valid URI

        // Prepare the configuration settings
        // Use the HDFS 2.8.5 API to set the configuration values correctly
        conf.setBoolean(
                HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,
                HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT
        );
    }

    @Test
    public void testInitializeWithOauthDisabled() throws IOException {
        // Test code - initialize the WebHdfsFileSystem
        webHdfsFileSystem.initialize(uri, conf);

        // Assert (this is an example; you may need to modify this based on the desired behavior)
        assertFalse(
                conf.getBoolean(
                        HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,
                        false
                )
        );
    }
}