package org.apache.hadoop.ipc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import javax.net.SocketFactory;
import java.net.InetSocketAddress;
import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class TestClientRpcTimeout {

    private static final String IPC_CLIENT_RPC_TIMEOUT_KEY = "ipc.client.rpc-timeout.ms"; 
    private static final String IPC_CLIENT_PING_KEY = "ipc.client.ping";
    private static final String IPC_CLIENT_PING_INTERVAL_KEY = "ipc.ping.interval";

    // Test for Client.getRpcTimeout(Configuration conf)
    @Test
    public void testClient_getRpcTimeout_withValidTimeoutConfigured() {
        Configuration conf = new Configuration();
        conf.setInt(IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);
        int rpcTimeout = Client.getRpcTimeout(conf);
        assertEquals("RPC timeout should be fetched from the configuration", 5000, rpcTimeout);
    }

    // Test for Client.getTimeout(Configuration conf) with deprecated behavior
    @Test
    public void testClient_getTimeout_withPingDisabled() {
        Configuration conf = new Configuration();
        conf.setInt(IPC_CLIENT_RPC_TIMEOUT_KEY, -1);
        conf.setBoolean(IPC_CLIENT_PING_KEY, false);
        conf.setInt(IPC_CLIENT_PING_INTERVAL_KEY, 60000); // Default value used in Hadoop 2.8.5 for the ping interval

        int timeout = Client.getTimeout(conf);
        assertEquals("Timeout should fallback to default ping interval when ping is disabled", 60000, timeout);
    }

    // Test for RPC.getRpcTimeout(Configuration conf)
    @Test
    public void testRPC_getRpcTimeout_withConfiguredValue() {
        Configuration conf = new Configuration();
        conf.setInt(IPC_CLIENT_RPC_TIMEOUT_KEY, 7000);
        int rpcTimeout = RPC.getRpcTimeout(conf);
        assertEquals("RPC timeout should be fetched from configuration", 7000, rpcTimeout);
    }

    // Test for RPC.waitForProtocolProxy(Class, long, InetSocketAddress, Configuration, long)
    @Test
    public void testRPC_waitForProtocolProxy_withTimeoutConfiguration() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(IPC_CLIENT_RPC_TIMEOUT_KEY, 3000);
        InetSocketAddress dummyAddress = new InetSocketAddress("localhost", 8080);

        try {
            RPC.waitForProtocolProxy(TestProtocol.class, 
                                     1L, 
                                     dummyAddress, 
                                     conf, 
                                     3000);
        } catch (IOException e) {
            assertTrue("IOException should not occur for this test scenario", false);
        }
    }

    // Test for RPC.getProtocolProxy(Class, long, InetSocketAddress, UserGroupInformation, Configuration, SocketFactory)
    @Test
    public void testRPC_getProtocolProxy_withTimeoutConfiguration() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt(IPC_CLIENT_RPC_TIMEOUT_KEY, 5000);
        InetSocketAddress dummyAddress = new InetSocketAddress("localhost", 8080);
        UserGroupInformation dummyUGI = UserGroupInformation.createRemoteUser("dummyUser");
        SocketFactory dummySocketFactory = SocketFactory.getDefault();

        try {
            RPC.getProtocolProxy(TestProtocol.class, 
                                 1L, 
                                 dummyAddress, 
                                 dummyUGI, 
                                 conf, 
                                 dummySocketFactory);
        } catch (IOException e) {
            assertTrue("IOException should not occur for this test scenario", false);
        }
    }
}

// Dummy protocol interface for testing
interface TestProtocol {}