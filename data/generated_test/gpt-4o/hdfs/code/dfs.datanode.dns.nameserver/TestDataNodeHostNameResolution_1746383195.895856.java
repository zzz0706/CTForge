package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.net.DNS;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestDataNodeHostNameResolution {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDataNodeHostNameResolutionWithProvidedConfiguration() throws Exception {
        // Step 1: Prepare a Configuration object with relevant configurations
        Configuration config = new HdfsConfiguration();
        config.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "default");
        config.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "127.0.0.1");

        // Retrieve the configuration values
        String dnsInterface = config.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);
        String nameServer = config.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY);

        // Step 2: Assert presence of Configuration values
        assertNotNull("DNS interface should be defined", dnsInterface);
        assertNotNull("DNS nameserver should be defined", nameServer);

        // Step 3: Resolve hostname using the DNS utility directly since DataNode.getHostName is private
        String resolvedHostName = DNS.getDefaultHost(dnsInterface, nameServer);

        // Step 4: Verify the hostname resolution
        assertNotNull("Resolved hostname should not be null", resolvedHostName);
        assertTrue("Resolved hostname should not be empty", !resolvedHostName.isEmpty());
    }
}