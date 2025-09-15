package org.apache.zookeeper.test;

import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.junit.Test;
import static org.mockito.Mockito.*;
import static org.junit.Assert.assertEquals;

public class ZooKeeperServerTest {

    @Test
    //test code
    // 1. You need to use the zookeeper3.5.6 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testGetSecureClientPortWithSecureServerFactoryPresent() {
        // Step 1: Set up conditions and mocks
        ServerCnxnFactory secureServerCnxnFactoryMock = mock(ServerCnxnFactory.class);
        int mockSecurePort = 2281; // Mock secure port value for testing
        ZooKeeperServer zooKeeperServer = spy(new ZooKeeperServer());
        
        doReturn(secureServerCnxnFactoryMock).when(zooKeeperServer).getSecureServerCnxnFactory();
        when(secureServerCnxnFactoryMock.getLocalPort()).thenReturn(mockSecurePort);

        // Step 2: Call the API to get secure client port based on mocked configuration
        int secureClientPort = zooKeeperServer.getSecureClientPort();

        // Step 3: Validate the result
        assertEquals("Expected secure client port should match the mocked port", mockSecurePort, secureClientPort);

        // Step 4: Clean up after testing
        secureServerCnxnFactoryMock = null;
        zooKeeperServer = null;
    }
}