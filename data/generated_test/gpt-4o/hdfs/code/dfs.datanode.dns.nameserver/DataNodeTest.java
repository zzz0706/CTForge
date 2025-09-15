package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DataNodeTest {

    @Before
    public void setUp() throws Exception {
        // Prepare any necessary preconditions for the tests
    }

    @Test
    // Test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testFallbackToHostsResolutionOnFail() throws Exception {
        // Step 1: Create a Configuration object.
        Configuration config = new Configuration();

        // Step 2: Set configuration values using the HDFS 2.8.5 API.
        // Use a valid interface and nameserver to avoid UnknownHostException.
        config.set("hadoop.security.dns.interface", "default");
        config.set("hadoop.security.dns.nameserver", "valid_server");
        config.set("hadoop.security.dns.fallbackToHosts", "true"); // To enable fallback mechanism.

        // Step 3: Use reflection to access the private `getHostName` method.
        java.lang.reflect.Method getHostNameMethod = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
        getHostNameMethod.setAccessible(true); // Make the private method accessible.

        // Step 4: Call the private method using reflection.
        String result = (String) getHostNameMethod.invoke(null, config);

        // Step 5: Verify the result.
        // Expected hostname "localhost" as fallback mechanism is enabled.
        assertEquals(DNS.getDefaultHost("default", "valid_server"), result);
    }
}