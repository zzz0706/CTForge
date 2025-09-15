package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Collection;

import static org.junit.Assert.*;

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
        conf.unset("dfs.datanode.dns.interface");
        conf.unset("dfs.datanode.keytab.file");
        conf.set("dfs.datanode.address", "127.0.0.1:50010");
        conf.set("dfs.datanode.http.address", "127.0.0.1:50075");
        conf.set("dfs.datanode.ipc.address", "127.0.0.1:50020");

        // Set up UserGroupInformation for secure resources.
        UserGroupInformation.setConfiguration(conf);

        try {
            // Attempt to use DNS utility to resolve hostname using missing configuration properties.
            String dnsInterface = DNS.getDefaultHost("dfs.datanode.dns.interface", null);
            conf.set("dfs.datanode.dns.address", dnsInterface);

            // Step 2: Attempt to instantiate the DataNode.
            DataNode.instantiateDataNode(new String[]{}, conf, null);
            fail("Expected exception due to missing configuration properties, but none was thrown.");
        } catch (UnknownHostException e) {
            // Step 3: Verify that exception message indicates missing DNS properties.
            String message = e.getMessage();
            assertTrue("Exception message should indicate missing DNS interface property.",
                    message.contains("dfs.datanode.dns.interface"));
        } catch (IOException e) {
            // Step 3: Verify that exception message indicates missing Kerberos properties.
            String message = e.getMessage();
            assertTrue("Exception message should indicate missing Kerberos property.",
                    message.contains("dfs.datanode.keytab.file"));
        }
    }
}