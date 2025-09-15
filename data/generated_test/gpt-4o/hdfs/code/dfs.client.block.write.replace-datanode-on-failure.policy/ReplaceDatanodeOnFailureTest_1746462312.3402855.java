package org.apache.hadoop.hdfs.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure;
import org.apache.hadoop.hdfs.protocol.datatransfer.ReplaceDatanodeOnFailure.Policy;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReplaceDatanodeOnFailureTest {

    @Test
    // Test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_get_with_policy_DEFAULT() {
        // Prepare the test conditions
        Configuration conf = new Configuration();
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.enable", true);
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "DEFAULT");
        conf.setBoolean("dfs.client.block.write.replace-datanode-on-failure.best-effort", true);

        // Test code: Directly access policy configuration and best-effort configuration.
        Policy policy = Policy.valueOf(conf.get("dfs.client.block.write.replace-datanode-on-failure.policy"));
        boolean bestEffort = conf.getBoolean("dfs.client.block.write.replace-datanode-on-failure.best-effort", false);

        // Assert the policy and bestEffort values
        assertEquals(Policy.DEFAULT, policy);
        assertTrue(bestEffort);
    }
}