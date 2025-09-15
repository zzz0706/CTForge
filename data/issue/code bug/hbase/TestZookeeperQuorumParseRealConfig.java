package org.apache.hadoop.hbase.zookeeper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;
//HBASE-13468
@Category({MiscTests.class, SmallTests.class})
public class TestZookeeperQuorumParseRealConfig {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestZookeeperQuorumParseRealConfig.class);

    @Test
    public void testParseQuorumFromRealConfig() {
        
        Configuration conf = HBaseConfiguration.create();
        
        
        String quorum = conf.get("hbase.zookeeper.quorum");
        assertNotNull("hbase.zookeeper.quorum should not be null", quorum);
        assertFalse("hbase.zookeeper.quorum should not be empty", quorum.trim().isEmpty());
      
        int port = conf.getInt("hbase.zookeeper.property.clientPort", 2181);

        String connectStr = ZKConfig.getZKQuorumServersString(conf);

        assertNotNull("connectString should not be null", connectStr);
        assertFalse("connectString should not be empty", connectStr.trim().isEmpty());

        String[] servers = quorum.split(",");
        for (String rawServer : servers) {
            String trimmed = rawServer.trim();
            boolean found = false;
            if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
                found = connectStr.contains(trimmed + ":" + port);
            } else {
                found = connectStr.contains(trimmed + ":" + port) || connectStr.contains(trimmed);
            }
            assertTrue(
                "connectString should correctly contain: " + trimmed + " with port: " + port,
                found
            );
        }
    }
}
