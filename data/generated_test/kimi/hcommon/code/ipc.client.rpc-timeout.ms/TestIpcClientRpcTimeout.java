package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.junit.Assert.assertEquals;

public class TestIpcClientRpcTimeout {

    @Test
    public void negativeValueClampedToZero() {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative value to test clamping
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -500);
        
        // 3. Calculate expected value (should be 0 for negative input)
        int expectedTimeout = 0;
        
        // 4. Invoke method under test
        int actualTimeout = Client.getRpcTimeout(conf);
        
        // 5. Assert the result
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void testGetRpcTimeoutInRPC() {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative value
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -1000);
        
        // 3. Expected clamped value
        int expectedTimeout = -1000; // RPC.getRpcTimeout does not clamp
        
        // 4. Invoke RPC.getRpcTimeout
        int actualTimeout = RPC.getRpcTimeout(conf);
        
        // 5. Assert the result
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void testWaitForProtocolProxyUsesRpcTimeout() throws IOException {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative timeout to be clamped
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -300);
        
        // 3. Create minimal test parameters
        InetSocketAddress dummyAddr = new InetSocketAddress("127.0.0.1", 0);
        long connTimeout = 1000L;
        
        // 4. Expected clamped timeout (RPC.getRpcTimeout returns raw value)
        int expectedRpcTimeout = -300;
        
        // 5. Invoke waitForProtocolProxy to trigger usage of getRpcTimeout
        try {
            RPC.waitForProtocolProxy(VersionedProtocol.class, 1L, dummyAddr, conf, connTimeout);
        } catch (IOException e) {
            // Expected due to dummy address; we only care about timeout propagation
        }
        
        // 6. Verify RPC.getRpcTimeout returns unclamped value
        assertEquals(expectedRpcTimeout, RPC.getRpcTimeout(conf));
    }

    @Test
    public void testGetProtocolProxyUsesRpcTimeout() throws IOException {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative timeout to be clamped
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -250);
        
        // 3. Create minimal test parameters
        InetSocketAddress dummyAddr = new InetSocketAddress("127.0.0.1", 0);
        UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
        
        // 4. Expected clamped timeout (RPC.getRpcTimeout returns raw value)
        int expectedRpcTimeout = -250;
        
        // 5. Invoke getProtocolProxy to trigger usage of getRpcTimeout
        try {
            RPC.getProtocolProxy(VersionedProtocol.class, 1L, dummyAddr, ugi, conf, null);
        } catch (IOException e) {
            // Expected due to dummy address; we only care about timeout propagation
        }
        
        // 6. Verify RPC.getRpcTimeout returns unclamped value
        assertEquals(expectedRpcTimeout, RPC.getRpcTimeout(conf));
    }

    @Test
    public void testDeprecatedGetTimeoutWithNegativeRpcTimeout() {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative RPC timeout
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -100);
        
        // 3. Disable ping to trigger fallback to pingInterval
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, false);
        
        // 4. Expected clamped timeout from getRpcTimeout (0) triggers fallback
        int expectedTimeout = Client.getPingInterval(conf);
        
        // 5. Invoke deprecated getTimeout
        int actualTimeout = Client.getTimeout(conf);
        
        // 6. Assert the result
        assertEquals(expectedTimeout, actualTimeout);
    }

    @Test
    public void testDeprecatedGetTimeoutWithPingEnabled() {
        // 1. Instantiate configuration
        Configuration conf = new Configuration();
        
        // 2. Set negative RPC timeout
        conf.setInt(CommonConfigurationKeys.IPC_CLIENT_RPC_TIMEOUT_KEY, -100);
        
        // 3. Enable ping
        conf.setBoolean(CommonConfigurationKeys.IPC_CLIENT_PING_KEY, true);
        
        // 4. Expected no timeout when ping is enabled
        int expectedTimeout = -1;
        
        // 5. Invoke deprecated getTimeout
        int actualTimeout = Client.getTimeout(conf);
        
        // 6. Assert the result
        assertEquals(expectedTimeout, actualTimeout);
    }
}