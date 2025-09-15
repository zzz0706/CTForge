package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ZooKeeperServerTest {

    @Test
    // Test code
    // 1. Directly call API to obtain configuration values, do not define configuration files and add them.
    // 2. Use the zookeeper3.5.6 API correctly to obtain configuration values, avoiding hardcoding configuration values.
    // 3. Prepare the test conditions.
    // 4. Execute the test code.
    // 5. Code after testing.

    public void testGetSecureClientPortWithSecureServerFactoryAbsent() {
        // Step 1: Prepare the test conditions
        // Create a ZooKeeperServer instance without initializing secureServerCnxnFactory
        ZooKeeperServer zooKeeperServer = new ZooKeeperServer();

        // Step 2: Execute the test code
        int secureClientPort = zooKeeperServer.getSecureClientPort();

        // Step 3: Verify the result
        // The return value must be -1, indicating that no secure port was initialized
        assertEquals("Secure client port should be -1 when secureServerCnxnFactory is not initialized.", -1, secureClientPort);
    }
}