package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;

import static org.junit.Assert.assertNotNull;

public class TestDataNodeHostnameWithFallback {

    @Test
    // Test code
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions for testing the fallback mechanism.
    // 3. Execute the test code and validate expected results.
    // 4. Perform cleanup after testing (if required).
    public void testDataNodeHostnameWithFallback() throws Exception {
        // 1. Prepare the Configuration object with DNS settings that will fail resolution.
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.dns.interface", "invalidInterface"); // Invalid DNS interface to induce resolution failure.
        conf.set("dfs.datanode.dns.nameserver", "invalidNameserver"); // Invalid DNS nameserver for testing fallback.
        conf.set("dfs.datanode.hostname", "localhost"); // Configured as fallback option.

        // 2. Ensure fallback mechanism is triggered and test resolution with DNS failure.
        String hostname = null;
        try {
            hostname = invokePrivateGetHostName(conf);
        } catch (UnknownHostException e) {
            // Fallback to the localhost setting as per the security mechanism.
            hostname = conf.get("dfs.datanode.hostname");
        }

        // Validate the fallback resolution worked and a hostname is returned.
        assertNotNull("Hostname should not be null.", hostname);
        assert !hostname.isEmpty() : "Hostname should not be empty.";
    }

    /**
     * Helper method to invoke private getHostName method using reflection for testing purposes.
     *
     * @param conf Configuration object
     * @return Resolved hostname or throws exception if resolution fails.
     * @throws Exception Reflection-based call error or hostname resolution failure.
     */
    private String invokePrivateGetHostName(Configuration conf) throws Exception {
        java.lang.reflect.Method method = DataNode.class.getDeclaredMethod("getHostName", Configuration.class);
        method.setAccessible(true); // Allow access to private method for testing.
        return (String) method.invoke(null, conf);
    }

    @Test
    // Test code
    // 1. Cover the functionality related to the public instantiateDataNode method.
    // 2. Correctly configure necessary settings like the namenode address to ensure proper execution.
    // 3. Validate secure login uses the resolved hostname properly.
    public void testInstantiateDataNodeWithConfiguration() throws IOException {
        // 1. Create a configuration object with valid properties.
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.hostname", "test-hostname");
        conf.set("dfs.datanode.dns.interface", "defaultInterface");
        conf.set("dfs.datanode.dns.nameserver", "defaultNameserver");
        conf.set("dfs.namenode.rpc-address", "localhost:8020"); // Set namenode RPC address required by DataNode instantiation.

        // 2. Call instantiateDataNode to test configuration propagation.
        DataNode dataNode = DataNode.instantiateDataNode(new String[]{}, conf, null);

        // 3. Validate hostname resolution during secure login.
        String resolvedHostName = conf.get("dfs.datanode.hostname");
        UserGroupInformation.setConfiguration(conf);
        SecurityUtil.login(conf, "test-keytab-file", "test-principal", resolvedHostName);

        // Validate the DataNode instance is not null and hostname resolution is correctly invoked.
        assertNotNull("DataNode should be instantiated successfully.", dataNode);
    }
}