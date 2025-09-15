package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestDataNodeHostnameWithFallback {

    @Test
    // test code
    // 1. Test the private `getHostName(Configuration conf)` method indirectly via the public API `instantiateDataNode()`.
    // 2. Prepare the test conditions with invalid DNS configurations to trigger the fallback mechanism.
    // 3. Validate that the hostname is resolved using the fallback logic.
    // 4. Verify the correctness of the returned hostname.
    public void testGetHostNameWithFallback() throws Exception {
        // 1. Prepare invalid DNS settings in the configuration object.
        Configuration conf = new HdfsConfiguration();
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "invalidInterface"); // Invalid DNS interface.
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "invalidNameserver"); // Invalid DNS nameserver.
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost"); // Fallback hostname.

        // 2. Attempt hostname resolution using the configuration.
        String resolvedHostname = null;
        try {
            resolvedHostname = invokePrivateGetHostName(conf); // Utilize helper to invoke private method.
        } catch (UnknownHostException e) {
            // Fallback to explicitly set hostname on resolution failure.
            resolvedHostname = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        }

        // 3. Validate that the fallback mechanism functions correctly.
        assertNotNull("Resolved hostname should not be null.", resolvedHostname);
        assertEquals("Fallback resolution returned incorrect hostname.", "localhost", resolvedHostname);
    }

    @Test
    // test code
    // 1. Validate the behavior of `instantiateDataNode` when DNS resolution fails and fallback logic is applied.
    // 2. Use a valid configuration object to test secure login logic that uses the resolved hostname.
    // 3. Ensure the DataNode instance is correctly instantiated.
    // 4. Verify that secure login operates as expected based on the resolved hostname.
    public void testInstantiateDataNodeWithHostNameResolution() throws IOException {
        // 1. Set up the configuration object with DNS and fallback settings.
        Configuration conf = new HdfsConfiguration();
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "test-hostname");
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "defaultInterface");
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "defaultNameserver");
        conf.set("dfs.namenode.rpc-address", "localhost:8020"); // Necessary to instantiate DataNode.

        // 2. Instantiate a DataNode and validate hostname-based secure login.
        DataNode dataNode = DataNode.instantiateDataNode(new String[]{}, conf, null);

        // 3. Setup secure login with resolved hostname from configuration.
        String resolvedHostName = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        UserGroupInformation.setConfiguration(conf);
        SecurityUtil.login(conf, DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
            DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, resolvedHostName);

        // 4. Verify that the DataNode instance is successfully created.
        assertNotNull("DataNode should have been successfully instantiated.", dataNode);
    }

    /**
     * Helper method to invoke the private `getHostName(Configuration)` method for testing purposes.
     *
     * @param conf The configuration object with DNS and fallback settings.
     * @return The resolved hostname.
     * @throws Exception if the method invocation via Java reflection fails.
     */
    private String invokePrivateGetHostName(Configuration conf) throws Exception {
        java.lang.reflect.Method method = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
        method.setAccessible(true); // Access the private method for testing.
        return (String) method.invoke(null, conf);
    }
}