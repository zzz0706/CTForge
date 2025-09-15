package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.*;

public class TestWebHdfsFileSystem {

    @Test
    // Test case: Validate that the WebHdfsFileSystem initialization gracefully handles
    // the absence of the dfs.webhdfs.socket.read-timeout configuration key and uses default timeout values.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initialize_without_valid_configuration_key() throws Exception {
        // 1. Prepare test conditions: Create a configuration object without dfs.webhdfs.socket.read-timeout
        Configuration conf = new Configuration();

        // 2. Define a WebHDFS URI
        URI uri = new URI("webhdfs://localhost:9870");

        // 3. Instantiate WebHdfsFileSystem
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // 4. Initialize the WebHdfsFileSystem with the URI and Configuration objects
        webHdfsFileSystem.initialize(uri, conf);

        // 5. Validate timeout configuration
        int defaultTimeout = conf.getInt(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
                30000); // Directly use an appropriate default timeout value (30000 ms)

        assertEquals("WebHdfsFileSystem should use the default timeout",
                30000, defaultTimeout); // Expected timeout value is 30000 ms

        // 6. Test cleanup
        webHdfsFileSystem.close();
    }
}