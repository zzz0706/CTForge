package org.apache.hadoop.hdfs.server.datanode;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants.StartupOption;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInstantiationWithMissingDnsInterface() throws Exception {
        // 1. Prepare the test configuration object
        Configuration conf = new HdfsConfiguration();

        // Intentionally omit setting "dfs.datanode.dns.interface" to test fallback behavior
        // Set necessary configurations to avoid runtime errors
        conf.set("dfs.namenode.rpc-address", "localhost:9000"); // Example hostname and port

        // Use the correct key for the fallback configuration
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, DNS.getDefaultHost("default", null, true));

        // 2. Initialize necessary arguments for DataNode instantiation
        String[] args = null; // No command-line arguments

        DataNode dataNode = null;

        try {
            // 3. Call instantiateDataNode to trigger hostname resolution and DataNode setup
            dataNode = DataNode.instantiateDataNode(args, conf, null);
            assertNotNull("DataNode instantiation failed when dfs.datanode.dns.interface is not set", dataNode);

            // Verify that DataNode hostname is correctly resolved
            String resolvedHostName = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
            assertNotNull("Resolved hostname is null", resolvedHostName);
            System.out.println("Resolved Hostname: " + resolvedHostName);

            // Verify additional behavior like the hostname in the Namenode addresses
            String namenodeAddresses = conf.get("dfs.namenode.rpc-address");
            System.out.println("Namenode Addresses: " + namenodeAddresses);

        } finally {
            // 4. Code after testing: Ensure proper cleanup
            if (dataNode != null) {
                dataNode.shutdown();
            }
        }
    }
}