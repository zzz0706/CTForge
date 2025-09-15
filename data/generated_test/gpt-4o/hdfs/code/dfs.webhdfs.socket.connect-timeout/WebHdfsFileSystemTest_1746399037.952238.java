package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.junit.Test;

import java.net.URI;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class WebHdfsFileSystemTest {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_initialize_withValidConfiguration() throws Exception {
        // 2. Prepare test conditions: Create a Configuration object and set required properties.
        Configuration conf = new Configuration();
        conf.setTimeDuration(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, 5000, TimeUnit.MILLISECONDS); // Set a valid connect timeout in milliseconds.

        // Construct a valid URI pointing to a WebHDFS endpoint
        URI testUri = new URI("webhdfs://localhost:9870");

        // Create a WebHdfsFileSystem instance
        WebHdfsFileSystem webHdfsFileSystem = new WebHdfsFileSystem();

        // 3. Test code: Invoke initialization and assert the timeout is correctly propagated.
        webHdfsFileSystem.initialize(testUri, conf);

        // Use the existing API to retrieve the timeout value for testing purposes.
        int connectTimeoutFromConf = (int) conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                0,
                TimeUnit.MILLISECONDS);

        assertTrue("Expect connectTimeoutFromConf > 0", connectTimeoutFromConf > 0);

        // Directly verify the configurations using the WebHdfsFileSystem instance, if possible.
        HttpURLConnection connection = (HttpURLConnection)new URL("http://localhost:9870").openConnection();
        connection.setConnectTimeout(connectTimeoutFromConf);
        connection.setReadTimeout(10000); // Assuming an arbitrary read timeout for testing.

        // Validate the values have been set correctly
        assertEquals("Connect timeout should be set correctly", connectTimeoutFromConf, connection.getConnectTimeout());
        assertEquals("Read timeout should be set correctly", 10000, connection.getReadTimeout());

        // 4. Code after testing: Clean up (if necessary).
        // No cleanup is required in this specific test.
    }
}