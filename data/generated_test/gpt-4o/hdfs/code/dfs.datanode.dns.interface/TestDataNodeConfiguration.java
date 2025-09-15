package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.DNS;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the HDFS 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInstantiationWithMissingDnsInterface() throws Exception {
        // 1. Prepare the test configuration object
        Configuration conf = new Configuration();

        // Intentionally omit setting "dfs.datanode.dns.interface" to test fallback behavior
        // Set necessary configurations to avoid runtime errors
        conf.set("dfs.namenode.rpc-address", "localhost:9000"); // Example hostname and port

        // Use the correct key for the fallback configuration
        conf.set("dfs.datanode.hostname", DNS.getDefaultHost("default"));

        // 2. Initialize necessary arguments for DataNode instantiation
        String[] args = null; // No command-line arguments

        // 3. Call instantiateDataNode to trigger hostname resolution and DataNode setup
        DataNode dataNode = null;
        try {
            dataNode = DataNode.instantiateDataNode(args, conf, null);
            assertNotNull("DataNode instantiation failed when dfs.datanode.dns.interface is not set", dataNode);

            // Additional Testing: Validate DataNode's behavior if applicable
            // For example, verify the default hostname or logging behavior
            
        } finally {
            // 4. Code after testing: Ensure proper cleanup
            if (dataNode != null) {
                dataNode.shutdown();
            }
        }
    }
}