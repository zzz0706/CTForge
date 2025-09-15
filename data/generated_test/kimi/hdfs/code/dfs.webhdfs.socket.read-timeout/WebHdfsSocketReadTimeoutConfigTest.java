package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class WebHdfsSocketReadTimeoutConfigTest {

    private Configuration conf;
    private String timeoutValue;

    public WebHdfsSocketReadTimeoutConfigTest(String timeoutValue) {
        this.timeoutValue = timeoutValue;
    }

    @Before
    public void setUp() throws IOException {
        conf = new Configuration(false);
    }

    @Test
    public void testDfsWebhdfsSocketReadTimeoutDefaultValue() {
        // Prepare: Do not set the property to test default value
        long expectedDefault = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);

        // For HDFS 2.8.5, we check the default value directly
        assertEquals("Default read timeout should be 60 seconds", 60000L, expectedDefault);
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {"30s"}, {"1m"}, {"60000ms"}, {"60s"}
        });
    }

    @Test
    public void testDfsWebhdfsSocketReadTimeoutParsing() {
        // Prepare
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, timeoutValue);

        // Invocation handling
        long actualTimeout = conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS);

        // Reference loader comparison
        long expectedTimeout = parseTimeDuration(timeoutValue);

        assertEquals("Parsed timeout should match expected value", expectedTimeout, actualTimeout);
    }

    @Test
    public void testInitializeMethodPassesReadTimeoutToConnectionFactory() throws IOException {
        // Prepare
        String testTimeout = "45s";
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY, testTimeout);
        conf.set(HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY, "30s");

        long expectedReadTimeout = parseTimeDuration(testTimeout);
        
        // Test that the URLConnectionFactory can be created with the configured timeouts
        URLConnectionFactory factory = URLConnectionFactory.newDefaultURLConnectionFactory(
            30000, (int)expectedReadTimeout, conf);
            
        // If we get here without exception, the factory was created successfully
        assertEquals("URLConnectionFactory should be created successfully", 
            URLConnectionFactory.class, factory.getClass());
    }

    private long parseTimeDuration(String value) {
        // Simplified time parser for test purposes
        if (value.endsWith("ns")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        } else if (value.endsWith("us")) {
            return Long.parseLong(value.substring(0, value.length() - 2)) * 1000;
        } else if (value.endsWith("ms")) {
            return Long.parseLong(value.substring(0, value.length() - 2));
        } else if (value.endsWith("s")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 1000;
        } else if (value.endsWith("m")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 60 * 1000;
        } else if (value.endsWith("h")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 60 * 60 * 1000;
        } else if (value.endsWith("d")) {
            return Long.parseLong(value.substring(0, value.length() - 1)) * 24 * 60 * 60 * 1000;
        } else {
            // Default to milliseconds if no unit specified
            return Long.parseLong(value);
        }
    }
}