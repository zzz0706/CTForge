package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.InetSocketAddress;
import static org.junit.Assert.*;
//hdfs-1321
public class NameNodeRpcConfigValidationTest {

    /**
     * Test that the dfs.namenode.rpc-address value is a valid "host:port" string.
     */
    @Test
    public void testRpcAddressFormat() {
        Configuration conf = new Configuration();
        String rpcAddress = conf.get("dfs.namenode.rpc-address", null);

        // If not configured, skip the test
        if (rpcAddress == null) return;

        // Pattern: host:port (host cannot contain whitespace, port is 1-65535)
        String pattern = "^[^\\s:]+:\\d{1,5}$";
        assertTrue(
            "dfs.namenode.rpc-address must be in the format 'host:port', but got: " + rpcAddress,
            rpcAddress.trim().matches(pattern)
        );
        // Optionally, parse to check port validity
        try {
            InetSocketAddress socket = parseAddress(rpcAddress);
            int port = socket.getPort();
            assertTrue("Port must be in range 1-65535", port >= 1 && port <= 65535);
        } catch (Exception e) {
            fail("dfs.namenode.rpc-address is not a valid host:port: " + e.getMessage());
        }
    }

    /**
     * Test that dfs.namenode.rpc-address and dfs.namenode.servicerpc-address
     * are not set to the same address (host:port).
     */
    @Test
    public void testRpcAndServiceRpcAddressDependency() {
        Configuration conf = new Configuration();
        String rpcAddress = conf.get("dfs.namenode.rpc-address", null);
        String serviceRpcAddress = conf.get("dfs.namenode.servicerpc-address", null);

        // Only check when both are configured
        if (rpcAddress == null || serviceRpcAddress == null) return;

        // Parse addresses
        InetSocketAddress rpcSocket, serviceSocket;
        try {
            rpcSocket = parseAddress(rpcAddress);
            serviceSocket = parseAddress(serviceRpcAddress);
        } catch (Exception e) {
            // If address format is wrong, skip dependency check
            return;
        }

        // They must not be the same (host and port must differ)
        assertFalse(
            "dfs.namenode.rpc-address and dfs.namenode.servicerpc-address must not be the same: " + rpcAddress,
            rpcSocket.equals(serviceSocket)
        );
    }

    // Utility: Parse "host:port" into InetSocketAddress
    private static InetSocketAddress parseAddress(String addr) {
        String[] parts = addr.trim().split(":");
        if (parts.length != 2) throw new IllegalArgumentException("Invalid address: " + addr);
        String host = parts[0].trim();
        int port = Integer.parseInt(parts[1].trim());
        return new InetSocketAddress(host, port);
    }
}
