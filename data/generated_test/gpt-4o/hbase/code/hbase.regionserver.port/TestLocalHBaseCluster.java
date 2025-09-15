package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestLocalHBaseCluster {

    @ClassRule // Ensures HBase test class rules, such as class-level setup and teardown
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestLocalHBaseCluster.class);

    @Test
    public void test_LocalHBaseCluster_random_ports_assignment() throws IOException {
        // 1. Prepare the test conditions: Initialize configuration object and enable random port assignment.
        Configuration conf = HBaseConfiguration.create();
        conf.setBoolean(HConstants.ASSIGN_RANDOM_PORTS, true); // Enable random port assignment.

        // 2. Test code: Initialize LocalHBaseCluster instance with configuration.
        LocalHBaseCluster cluster = new LocalHBaseCluster(
            conf,
            1,  // Single master node
            1,  // Single region server node
            null,  // Default master class
            null   // Default region server class
        );

        // Assertions: Verify that the port properties are updated for random port assignment.
        assertEquals(
            "0",
            conf.get(HConstants.MASTER_PORT, null),
            "Expected master port to be set to '0' under random port assignment."
        );
        assertEquals(
            "0",
            conf.get(HConstants.REGIONSERVER_PORT, null),
            "Expected region server port to be set to '0' under random port assignment."
        );

        // 3. Code after testing: Cleanup or any additional test dependencies.
        cluster.shutdown(); // Cleanly shut down the local HBase cluster after testing.
    }
}