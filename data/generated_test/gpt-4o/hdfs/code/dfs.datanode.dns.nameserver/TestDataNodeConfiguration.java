package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.DNS;
import org.junit.Assert;
import org.junit.Test;

public class TestDataNodeConfiguration {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_getHostName_withLegacyDnsConfiguration() throws Exception {
        // Step 1: Create a Configuration object and prepare test conditions.
        Configuration conf = new Configuration();

        // Correctly set the DNS interface and nameserver using DFSConfigKeys constants.
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY, "default"); // Example value; test using valid system interface name.
        conf.set(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY, "8.8.8.8"); // Example value; test using a reachable nameserver.

        // Step 2: Test code.
        try {
            // Use the DNS utility to resolve the hostname based on the provided configuration values.
            String resolvedHostName = DNS.getDefaultHost(
                conf.get(DFSConfigKeys.DFS_DATANODE_DNS_INTERFACE_KEY),
                conf.get(DFSConfigKeys.DFS_DATANODE_DNS_NAMESERVER_KEY)
            );

            // Assert that the hostname is resolved successfully.
            Assert.assertNotNull("Resolved hostname should not be null", resolvedHostName);
            Assert.assertFalse("Resolved hostname should not be empty", resolvedHostName.isEmpty());

            // Debugging output for verification.
            System.out.println("Resolved Hostname: " + resolvedHostName);
        } catch (Exception e) {
            // Handle all exceptions to avoid test failures due to unexpected errors.
            Assert.fail("Exception occurred during DNS resolution: " + e.getMessage());
        }

        // Step 3: Code after testing.
        // Clean-up is not required for this specific test case.
    }
}