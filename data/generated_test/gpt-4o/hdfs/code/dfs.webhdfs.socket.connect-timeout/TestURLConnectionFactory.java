package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.URLConnectionFactory;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URLConnection;

import static org.mockito.Mockito.*;

/**
 * Test class to verify functionality of URLConnectionFactory's setTimeouts method.
 */
public class TestURLConnectionFactory {

    @Test
    // Test case: test_setTimeouts_appliesCorrectTimeouts
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testSetTimeoutsAppliesCorrectTimeouts() throws Exception {
        // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values.
        Configuration conf = new Configuration();
        conf.setInt("dfs.webhdfs.socket.connect-timeout", 60000);
        conf.setInt("dfs.webhdfs.socket.read-timeout", 60000);

        final int connectTimeout = conf.getInt(
                "dfs.webhdfs.socket.connect-timeout",
                60000  // Default value
        );
        final int readTimeout = conf.getInt(
                "dfs.webhdfs.socket.read-timeout",
                60000  // Default value
        );

        // Create an instance of URLConnectionFactory.
        URLConnectionFactory urlConnectionFactory = URLConnectionFactory.newDefaultURLConnectionFactory(conf);

        // Use reflection to access and invoke the private `setTimeouts` method.
        URLConnection mockConnection = mock(HttpURLConnection.class); // Mock URLConnection.

        java.lang.reflect.Method setTimeoutsMethod = URLConnectionFactory.class.getDeclaredMethod(
                "setTimeouts", URLConnection.class, int.class, int.class);
        setTimeoutsMethod.setAccessible(true); // Make private method accessible.

        // 2. Prepare the test conditions and invoke the method under test.
        setTimeoutsMethod.invoke(urlConnectionFactory, mockConnection, connectTimeout, readTimeout);

        // 3. Verify correctness using Mockito.
        verify(mockConnection).setConnectTimeout(connectTimeout);
        verify(mockConnection).setReadTimeout(readTimeout);

        // 4. Code after testing.
        // Ensure no exceptions occur during verification (implicit by the absence of thrown exceptions).
    }
}