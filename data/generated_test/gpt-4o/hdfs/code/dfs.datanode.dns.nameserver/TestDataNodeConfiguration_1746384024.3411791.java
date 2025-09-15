package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.hdfs.DFSConfigKeys;

import org.junit.Test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testInstantiateDataNodeWithMissingConfiguration() {
        // Step 1: Prepare the Configuration object, intentionally omitting specific properties.
        Configuration conf = new HdfsConfiguration();

        // Unset and set necessary properties intentionally for the test.
        conf.unset(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY);
        conf.unset(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY);
        conf.set(DFSConfigKeys.DFS_DATANODE_ADDRESS_KEY, "127.0.0.1:50010");
        conf.set(DFSConfigKeys.DFS_DATANODE_HTTP_ADDRESS_KEY, "127.0.0.1:50075");
        conf.set(DFSConfigKeys.DFS_DATANODE_IPC_ADDRESS_KEY, "127.0.0.1:50020");

        // Set up UserGroupInformation for secure resources.
        UserGroupInformation.setConfiguration(conf);

        try {
            // Attempt to use DNS utility to resolve hostname using missing configuration properties.
            String dnsInterface = DNS.getDefaultHost(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, null);
            conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, dnsInterface);

            // Step 2: Attempt to instantiate the DataNode.
            DataNode.instantiateDataNode(new String[]{}, conf, null);
            fail("Expected exception due to missing configuration properties, but none was thrown.");
        } catch (UnknownHostException e) {
            // Step 3: Verify that exception message indicates missing DNS properties.
            String message = e.getMessage();
            assertTrue("Exception message should indicate missing DNS interface property.",
                    message.contains(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY));
        } catch (IOException e) {
            // Step 3: Verify that exception message indicates missing Kerberos properties.
            String message = e.getMessage();
            assertTrue("Exception message should indicate missing Kerberos property.",
                    message.contains(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY));
        }
    }
}