package org.apache.hadoop.hbase.master.balancer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer.Cluster;
import org.apache.hadoop.hbase.master.balancer.BaseLoadBalancer;
import org.apache.hadoop.hbase.master.balancer.RegionLocationFinder;
import org.apache.hadoop.hbase.master.RackManager;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Deque;

import static org.junit.Assert.assertTrue;

@Category({org.apache.hadoop.hbase.testclassification.MasterTests.class,
        org.apache.hadoop.hbase.testclassification.SmallTests.class})
public class TestBaseLoadBalancerNeedsBalance {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestBaseLoadBalancerNeedsBalance.class);

    @Test
    public void testNeedsBalanceExceedsThreshold() throws Exception {
        // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.setFloat("hbase.master.balancer.max.skew", 0.2f);

        // Set up a concrete subclass of BaseLoadBalancer for testing
        BaseLoadBalancer baseLoadBalancer = new SimpleLoadBalancer();
        baseLoadBalancer.setConf(conf);

        // Fetch the slop configuration value correctly using the configuration key instead of non-existent constant
        float slop = conf.getFloat("hbase.master.balancer.max.skew", 0.2f);

        // Ensure slop value remains between 0 and 1 as per BaseLoadBalancer's design
        if (slop < 0) slop = 0;
        if (slop > 1) slop = 1;

        // Prepare a mock Cluster information
        Map<ServerName, List<RegionInfo>> serverMap = new HashMap<>();
        ServerName server1 = ServerName.valueOf("server1.example.org", 60010, System.currentTimeMillis() + 10);
        ServerName server2 = ServerName.valueOf("server2.example.org", 60020, System.currentTimeMillis() + 20);

        List<RegionInfo> server1Regions = createMockRegionInfo(20); // Overloaded server
        List<RegionInfo> server2Regions = createMockRegionInfo(5);  // Underloaded server

        serverMap.put(server1, server1Regions);
        serverMap.put(server2, server2Regions);

        // Instantiate RegionLocationFinder and RackManager since they are required for Cluster
        RegionLocationFinder regionLocationFinder = new RegionLocationFinder();
        RackManager rackManager = new RackManager(conf);

        // Create the Cluster object using the correct constructor
        Cluster cluster = new Cluster(null, serverMap, new HashMap<>(), regionLocationFinder, rackManager);

        // 3. Test code: Invoke the needsBalance method
        boolean needsBalance = baseLoadBalancer.needsBalance(cluster);

        // Assert that balance must be triggered due to load disparity
        assertTrue("Balance should be triggered when the load threshold is exceeded.", needsBalance);
    }

    // Helper method to generate mock RegionInfo objects
    private List<RegionInfo> createMockRegionInfo(int count) {
        List<RegionInfo> regionInfos = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            regionInfos.add(RegionInfoBuilder.newBuilder(TableName.valueOf("testTable"))
                    .setStartKey(("key" + i).getBytes())
                    .setEndKey(("key" + (i + 1)).getBytes())
                    .setRegionId(i)
                    .build());
        }
        return regionInfos;
    }
}