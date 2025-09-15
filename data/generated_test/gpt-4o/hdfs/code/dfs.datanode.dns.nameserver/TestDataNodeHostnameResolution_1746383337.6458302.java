package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.net.DNS;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class TestDataNodeHostnameResolution {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDataNodeHostnameResolutionWithEmptyConfiguration() throws Exception {
        // Prepare the test conditions: Create an empty Configuration object.
        Configuration config = new Configuration();

        // Add a valid configuration property for the hostname resolution.
        config.set(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY, DNS.getDefaultHost("default", "default"));

        // Test code: Get the hostname using the configuration.
        String resolvedHostName = config.get(DFSConfigKeys.DFS_DATANODE_HOST_NAME_KEY);

        // Code after testing: Validate that the resolved hostname is not null or empty.
        assertNotNull("The resolved hostname should not be null.", resolvedHostName);
        assertNotNull("The resolved hostname should be a valid string.", !resolvedHostName.isEmpty());
    }
}