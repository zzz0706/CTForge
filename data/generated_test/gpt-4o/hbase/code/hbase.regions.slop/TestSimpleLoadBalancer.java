package org.apache.hadoop.hbase.master.balancer;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.master.RegionPlan;
import org.apache.hadoop.hbase.master.balancer.SimpleLoadBalancer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNull;

@Category({org.apache.hadoop.hbase.testclassification.MasterTests.class, 
           org.apache.hadoop.hbase.testclassification.SmallTests.class})
public class TestSimpleLoadBalancer {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestSimpleLoadBalancer.class);

    @Test
    public void testBalanceClusterSingleRegionServer() throws Exception {
        // 1. Use the HBase 2.2.2 API correctly to obtain configuration values.
        Configuration conf = new Configuration();
        
        // Initialize SimpleLoadBalancer with configuration.
        SimpleLoadBalancer loadBalancer = new SimpleLoadBalancer();
        loadBalancer.setConf(conf);

        // 2. Prepare the test conditions.
        // Create a mocked single RegionServer with a single region.
        ServerName serverName = ServerName.valueOf("mockserver.example.com", 12345, System.currentTimeMillis());
        
        // Specify a proper TableName to create a mocked RegionInfo.
        TableName tableName = TableName.valueOf("test-table");
        RegionInfo regionInfo = RegionInfoBuilder.newBuilder(tableName).build();

        Map<ServerName, List<RegionInfo>> clusterMap = new HashMap<>();
        clusterMap.put(serverName, Collections.singletonList(regionInfo));

        // 3. Test code: invoke the method under test.
        List<RegionPlan> regionPlans = loadBalancer.balanceCluster(clusterMap);

        // 4. Validate the results.
        assertNull("balanceCluster should return null when there is only one RegionServer", regionPlans);
    }
}