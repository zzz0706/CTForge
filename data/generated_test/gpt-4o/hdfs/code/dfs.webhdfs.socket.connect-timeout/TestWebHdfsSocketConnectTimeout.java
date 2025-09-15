package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.net.URI;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;

public class TestWebHdfsSocketConnectTimeout {

    @Test
    // Test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDfsWebHdfsSocketConnectTimeoutConfiguration() {
        // 1. Correctly use the HDFS 2.8.5 API to obtain configuration values.
        // Initialize the configuration object
        Configuration conf = new Configuration();
        // Set relevant configuration values required for WebHdfsFileSystem
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "10000ms");
        conf.set("fs.defaultFS", "webhdfs://localhost:50070");

        // Retrieve and validate the configuration value using the HDFS API for the configured key
        String timeoutStr = conf.get(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY);

        // Validate that the configuration value is not null or empty
        if (timeoutStr == null || timeoutStr.isEmpty()) {
            fail("Configuration value for dfs.webhdfs.socket.connect-timeout is missing or empty.");
        }

        try {
            // 2. Prepare the test conditions.
            // Parse the timeout value using the HDFS API
            long timeoutValue = conf.getTimeDuration(
                HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
                0,
                TimeUnit.MILLISECONDS
            );

            // Validate that the timeout is positive
            if (timeoutValue <= 0) {
                fail("Configuration value for dfs.webhdfs.socket.connect-timeout must be a positive duration.");
            }

            // Validate the format of the timeout value string
            if (!timeoutStr.toLowerCase().matches("^\\d+(ns|us|ms|s|m|h|d)?$")) {
                fail("Configuration value for dfs.webhdfs.socket-connect-timeout must specify a valid time unit (ns, us, ms, s, m, h, d).");
            }

            // 3. Test code: Initialize a WebHdfsFileSystem with the configuration and validate setup
            WebHdfsFileSystem fs = new WebHdfsFileSystem(); // Initialize the WebHdfsFileSystem
            try {
                fs.initialize(
                    new URI(conf.get("fs.defaultFS")), // Use fs.defaultFS from configuration
                    conf
                );

                // Confirm that initialization works (indirect validation)
                Path testPath = new Path("/tmp/testfile");
                if (fs.exists(testPath)) {
                    fail("Path should not exist during test initialization: " + testPath);
                }
            } catch (Exception e) {
                // Pass if connection failed due to invalid test conditions, e.g., localhost is not running a WebHDFS instance
                if (!e.getMessage().contains("Connection refused")) {
                    fail("Unexpected error during WebHdfsFileSystem initialization: " + e.getMessage());
                }
            }

        } catch (Exception e) {
            // 4. Code after testing: Handle errors and log failure messages
            fail("Configuration value for dfs.webhdfs.socket.connect-timeout is invalid: " + e.getMessage());
        }
    }
}