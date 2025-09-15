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

public class TestClientAndRpcConfigurationUsage {
    
    // Test Client.getRpcTimeout(Configuration conf)
    @Test
    public void testClient_getRpcTimeout() {
        Configuration conf = new Configuration();
        conf.setInt("ipc.client.rpc-timeout.ms", 5000); // Set the RPC timeout
        
        int rpcTimeout = Client.getRpcTimeout(conf); // Fetch the timeout using the API
        assertEquals("The RPC timeout should match the value set in the configuration", 5000, rpcTimeout);
    }
    
    // Test Client.getTimeout(Configuration conf) using deprecated behavior
    @Test
    public void testClient_getTimeout() {
        Configuration conf = new Configuration();
        conf.setInt("ipc.client.rpc-timeout.ms", -1); // Negative RPC timeout to test fallback
        conf.setBoolean("ipc.client.ping", false);   // Disable ping
        conf.setInt("ipc.ping.interval", 60000);     // Define ping interval
        
        int timeout = Client.getTimeout(conf); // Fetch the timeout using the deprecated API
        assertEquals("Timeout should fallback to ping interval when ping is disabled", 60000, timeout);
    }

    // Test RPC.getRpcTimeout(Configuration conf)
    @Test
    public void testRPC_getRpcTimeout() {
        Configuration conf = new Configuration();
        conf.setInt("ipc.client.rpc-timeout.ms", 7000); // Set the RPC timeout
        
        int rpcTimeout = RPC.getRpcTimeout(conf); // Fetch the timeout using RPC API
        assertEquals("The RPC timeout should match the value set in the configuration", 7000, rpcTimeout);
    }

    // Test RPC.waitForProtocolProxy(Class, long, InetSocketAddress, Configuration, long)
    @Test
    public void testRPC_waitForProtocolProxy() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("ipc.client.rpc-timeout.ms", 3000); // Set the RPC timeout
        InetSocketAddress dummyAddress = new InetSocketAddress("localhost", 8080);

        try {
            RPC.waitForProtocolProxy(TestProtocol.class, 
                                     1L, 
                                     dummyAddress, 
                                     conf, 
                                     3000); // Use the timeout in proxy initialization
        } catch (IOException e) {
            assertTrue("IOException should not occur during protocol proxy setup", false);
        }
    }

    // Test RPC.getProtocolProxy(Class, long, InetSocketAddress, UserGroupInformation, Configuration, SocketFactory)
    @Test
    public void testRPC_getProtocolProxy() throws IOException {
        Configuration conf = new Configuration();
        conf.setInt("ipc.client.rpc-timeout.ms", 5000); // Set the RPC timeout
        InetSocketAddress dummyAddress = new InetSocketAddress("localhost", 8080);
        UserGroupInformation dummyUGI = UserGroupInformation.createRemoteUser("dummyUser");
        SocketFactory dummySocketFactory = SocketFactory.getDefault();

        try {
            RPC.getProtocolProxy(TestProtocol.class, 
                                 1L, 
                                 dummyAddress, 
                                 dummyUGI, 
                                 conf, 
                                 dummySocketFactory); // Use the timeout in proxy initialization
        } catch (IOException e) {
            assertTrue("IOException should not occur during protocol proxy setup", false);
        }
    }
}

// Dummy protocol interface for test scenario
interface TestProtocol {}