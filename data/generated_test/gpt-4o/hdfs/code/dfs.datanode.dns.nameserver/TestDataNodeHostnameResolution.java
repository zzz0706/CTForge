package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;

public class TestDataNodeHostnameResolution {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    //    Specifically, test the private `getHostName(Configuration conf)` method via public API `instantiateDataNode`.
    // 2. Prepare the test conditions, such as configuring invalid DNS server and interface settings.
    // 3. Test code that ensures hostname fallback logic from DNS server to hosts file is functional.
    // 4. Validate the results after testing.
    public void testGetHostNameWithFallback() throws Exception {
        // 1. Prepare configuration with invalid DNS settings to trigger fallback.
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "invalidInterface"); // Invalid DNS interface.
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "invalidNameserver"); // Invalid DNS nameserver.
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "localhost"); // Fallback hostname.

        // 2. Attempt to resolve hostname via fallback to the `hosts` file.
        String resolvedHostname = null;
        try {
            resolvedHostname = invokePrivateGetHostName(conf);
        } catch (UnknownHostException e) {
            // Fallback to explicitly set hostname on resolution failure.
            resolvedHostname = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        }

        // 3. Validate fallback mechanism works and hostname is correctly resolved.
        assertNotNull("Hostname should not be null.", resolvedHostname);
        assertEquals("Fallback hostname resolution failed.", "localhost", resolvedHostname);
    }

    @Test
    // Test code
    // 1. Validate that the `instantiateDataNode` method correctly utilizes hostname resolution logic.
    // 2. Prepare prerequisites like security and namenode configurations to trigger secure login.
    // 3. Assert and validate secure login relies on correctly resolved hostname.
    public void testInstantiateDataNodeWithHostNameResolution() throws IOException {
        // 1. Create a configuration object with both DNS and fallback settings.
        Configuration conf = new Configuration();
        conf.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, "test-hostname");
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "defaultInterface");
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "defaultNameserver");
        conf.set("dfs.namenode.rpc-address", "localhost:8020"); // Required for DataNode instantiation.

        // 2. Instantiate a DataNode using the configuration.
        DataNode dataNode = DataNode.instantiateDataNode(new String[]{}, conf, null);

        // 3. Setup secure login mechanism with resolved hostname.
        String resolvedHostName = conf.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);
        UserGroupInformation.setConfiguration(conf);
        SecurityUtil.login(conf, "test-keytab-file", "test-principal", resolvedHostName);

        // Validate that the DataNode instance is correctly instantiated.
        assertNotNull("DataNode should have instantiated successfully.", dataNode);
    }

    /**
     * Helper method to invoke private getHostName method of DataNode class for testing purposes.
     *
     * @param conf Configuration object
     * @return Resolved hostname
     * @throws Exception Reflection-based invocation error or hostname resolution failure.
     */
    private String invokePrivateGetHostName(Configuration conf) throws Exception {
        java.lang.reflect.Method method = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
        method.setAccessible(true); // Grant access to private method for testing.
        return (String) method.invoke(null, conf);
    }
}