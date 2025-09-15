package org.apache.hadoop.net;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import javax.net.SocketFactory;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public class TestNetUtils {

    // Test code
    // 1. Use API to fetch configuration values instead of hardcoding
    // 2. Prepare testing conditions
    // 3. Write test code
    // 4. Post-test verifications

    // Modify TIME_FUDGE_MILLIS to be protected instead of private to avoid access errors
    protected static final int TIME_FUDGE_MILLIS = 10; // Changed access modifier

    @Test
    public void testGetSocketFactoryFromProperty_withValidClass() {
        // Prepare testing condition: get a Configuration instance
        Configuration conf = new Configuration();

        // Pass a valid custom SocketFactory implementation class
        String customSocketFactoryClass = "org.apache.hadoop.net.StandardSocketFactory";

        // Fetch the SocketFactory using the NetUtils API
        SocketFactory socketFactory = NetUtils.getSocketFactoryFromProperty(conf, customSocketFactoryClass);

        // Verify the resulting SocketFactory matches the expected implementation
        assertTrue("The socket factory should be an instance of the expected class",
                socketFactory != null && socketFactory.getClass().getName().equals(customSocketFactoryClass));
    }
}