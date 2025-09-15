package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.DNS;
import org.junit.Test;
import static org.junit.Assert.*;

import java.net.UnknownHostException;

public class TestDatanodeDnsInterfaceConfiguration {

    /**
     * Validates the dfs.datanode.dns.interface configuration value and checks that it meets constraints and dependencies.
     */
    @Test
    public void testDfsDatanodeDnsInterfaceConfiguration() throws UnknownHostException {
        // Step 1: Prepare the test environment
        Configuration conf = new Configuration();

        // Step 2: Retrieve the value of dfs.datanode.dns.interface
        String dnsInterface = conf.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_DEFAULT);

        // Step 3: Assert the configuration validity
        try {
            // Retrieve hostname based on the dns interface
            String hostname = DNS.getDefaultHost(dnsInterface, null);
            
            // Assert that the hostname is not null or empty
            assertNotNull("Hostname should not be null when resolved from dns interface.", hostname);
            assertFalse("Hostname should not be empty when resolved from dns interface.", hostname.isEmpty());
        } catch (UnknownHostException e) {
            // If the hostname cannot be resolved, then dfs.datanode.dns.interface might not be valid
            fail("Unable to determine hostname from the specified dfs.datanode.dns.interface: " + e.getMessage());
        }

        // Step 4: Ensure compatibility with hadoop.security.dns.interface (preferred if set)
        String securityDnsInterface = conf.get("hadoop.security.dns.interface");
        if (securityDnsInterface != null) {
            assertFalse("When hadoop.security.dns.interface is set, dfs.datanode.dns.interface should be empty or used for fallback configuration.",
                    dnsInterface != null && !dnsInterface.isEmpty());
        }
    }
}