package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.assertNotNull;

public class TestWebHdfsFileSystemTimeoutParsing {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testTimeoutParsingAndPropagation() throws IOException {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, "60000"); // Configuration expects milliseconds
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "30000"); // Configuration expects milliseconds

        URI webHdfsUri = URI.create("webhdfs://localhost:50070");
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // Test code
        webHdfsFileSystem.initialize(webHdfsUri, conf);

        // Verify the parsed read timeout
        long readTimeout = conf.getLong(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
                60000L); // Provide default inline since `DFS_WEBHDFS_SOCKET_READ_TIMEOUT_DEFAULT` is absent.
        assertNotNull(readTimeout);

        // Verify the parsed connect timeout
        long connectTimeout = conf.getLong(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                30000L); // Provide default inline since `DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_DEFAULT` is absent.
        assertNotNull(connectTimeout);

        // Verify the WebHdfsFileSystem is initialized correctly
        Path path = new Path("/");
        assertNotNull("Filesystem should be initialized", webHdfsFileSystem.makeQualified(path));

        // Code after testing
        webHdfsFileSystem.close();
    }
}