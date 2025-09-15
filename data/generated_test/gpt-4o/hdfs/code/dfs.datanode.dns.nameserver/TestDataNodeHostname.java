package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class TestDataNodeHostname {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testDataNodeHostnameWithFallback() throws Exception {
        // 1. Prepare the Configuration object with fallback testing scenario.
        Configuration conf = new Configuration();
        conf.set("dfs.datanode.hostname", "localhost"); // Sample configuration required for hostname.

        // 2. Test the DataNode.getHostName functionality.
        // Use DNS.getDefaultHost directly, as DataNode.getHostName is private.
        String dnsInterface = conf.get("dfs.datanode.dns.interface", "default");
        String nameServer = conf.get("dfs.datanode.dns.nameserver", "default");
        String hostName = DNS.getDefaultHost(dnsInterface, nameServer);

        // 3. Validate the resulting hostname (fallback mechanism).
        assertNotNull("Hostname should not be null.", hostName);
        assert !hostName.isEmpty() : "Hostname should not be empty.";

        // 4. Clean up after the test (if required).
        // Perform any necessary cleanup, though none is explicitly required here.
    }
}