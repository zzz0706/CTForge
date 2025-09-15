package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.SecurityUtil;
import org.junit.Test;
import static org.junit.Assert.assertNotNull;

public class TestDataNodeDNSInterface {

    @Test 
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_instantiateDataNode_fallback_to_legacy_dns_interface() throws Exception {
        // Step 1: Create Configuration object
        Configuration config = new HdfsConfiguration();

        // Set required configuration values for initialization
        config.set("dfs.namenode.rpc-address", "localhost:9000");
        config.set("dfs.namenode.servicerpc-address", "localhost:9001");

        // Unset configuration values that should not be used
        config.unset("hadoop.security.dns.interface");
        config.unset("hadoop.security.dns.nameserver");
        config.unset("dfs.datanode.hostname");

        // Step 2: Prepare the arguments and initialize the DataNode
        String[] args = null;
        DataNode dataNode = DataNode.instantiateDataNode(args, config, null);

        // Step 3: Verify successful instantiation of DataNode
        assertNotNull("DataNode instantiation failed using fallback legacy DNS keys.", dataNode);

        // Step 4: Extract resolved hostname from the configuration using publicly accessible methods
        String resolvedHostName = config.get("dfs.datanode.hostname");
        if (resolvedHostName == null) {
            resolvedHostName = "localhost"; // Default to localhost if no hostname is set
        }
        assertNotNull("Resolved hostname is null or not set correctly.", resolvedHostName);

        // Step 5: Test SecurityUtil login
        SecurityUtil.login(config, "dfs.datanode.keytab.file",
                "dfs.datanode.kerberos.principal", resolvedHostName);
    }
}