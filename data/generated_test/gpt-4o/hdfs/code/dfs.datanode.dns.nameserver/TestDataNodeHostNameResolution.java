package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestDataNodeHostNameResolution {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDataNodeHostNameResolutionWithLegacyConfiguration() throws Exception {
        // Step 1: Prepare Configuration object
        Configuration config = new Configuration();

        // Set necessary configuration values for testing
        config.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "default");
        config.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "127.0.0.1");

        // Retrieve configuration values
        String dnsInterface = config.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);
        String nameServer = config.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY);

        // Step 2: Verify Configuration values retrieved successfully
        assertNotNull("DNS interface should be defined", dnsInterface);
        assertNotNull("DNS nameserver should be defined", nameServer);

        // Step 3: Invoke the hostname resolution logic for testing
        String resolvedHostName = DNS.getDefaultHost(dnsInterface, nameServer, true);

        // Step 4: Verify the hostname resolution
        assertNotNull("Resolved hostname should not be null", resolvedHostName);
    }
}