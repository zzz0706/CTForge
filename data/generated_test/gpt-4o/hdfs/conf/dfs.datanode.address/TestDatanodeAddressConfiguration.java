package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.NetUtils;
import org.junit.Test;

import java.net.InetSocketAddress;

import static org.junit.Assert.*;

/**
 * Unit test for validating the configuration of dfs.datanode.address in HDFS 2.8.5
 */
public class TestDatanodeAddressConfiguration {

    @Test
    /**
     * Test to verify if the configuration dfs.datanode.address is valid.
     * This includes checking its constraints and ensuring it is properly configured in the system.
     */
    public void testDfsDatanodeAddressConfiguration() {
        // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        
        // 2. Prepare the test conditions.
        String datanodeAddress = conf.getTrimmed(
                DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY,
                DFSConfigKeys.DFS_DATANODE_ADDRESS_DEFAULT);

        // 3. Validate and test the configuration value.
        // 3.1 Ensure the value is not null or empty.
        assertNotNull("dfs.datanode.address should not be null", datanodeAddress);
        assertFalse("dfs.datanode.address should not be empty", datanodeAddress.isEmpty());

        // 3.2 Validate that the address can be correctly parsed into a socket address.
        InetSocketAddress socketAddress = null;
        try {
            socketAddress = NetUtils.createSocketAddr(datanodeAddress);
        } catch (IllegalArgumentException e) {
            fail("Invalid dfs.datanode.address: " + datanodeAddress);
        }

        assertNotNull("The parsed InetSocketAddress should not be null", socketAddress);

        // 3.3 Ensure the port is within the valid range for TCP/UDP ports.
        int port = socketAddress.getPort();
        assertTrue("The port in dfs.datanode.address should be between 1 and 65535",
                port > 0 && port <= 65535);

        // 3.4 Ensure that the hostname is valid.
        String host = socketAddress.getHostName();
        assertNotNull("The hostname in dfs.datanode.address should not be null", host);
        assertFalse("The hostname in dfs.datanode.address should not be empty", host.isEmpty());
    }
}