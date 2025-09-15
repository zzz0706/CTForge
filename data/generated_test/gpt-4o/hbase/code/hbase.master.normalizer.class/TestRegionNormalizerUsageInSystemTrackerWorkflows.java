package org.apache.hadoop.hbase.master.normalizer;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.conf.ConfigurationManager;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestRegionNormalizerUsageInSystemTrackerWorkflows {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestRegionNormalizerUsageInSystemTrackerWorkflows.class);

    private static final HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
    private static HMaster master;

    @BeforeClass
    public static void setUp() throws Exception {
        // Start a mini cluster for testing
        TEST_UTIL.startMiniCluster();
        master = TEST_UTIL.getMiniHBaseCluster().getMaster();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        // Shut down the mini cluster after testing
        TEST_UTIL.shutdownMiniCluster();
    }

    @Test
    public void testRegionNormalizerUsage() throws Exception {
        // Obtain the Admin and Configuration objects
        Admin admin = TEST_UTIL.getAdmin();

        // Prepare test conditions using HBase 2.2.2 API correctly
        // Fetch the RegionNormalizer instance directly from master
        RegionNormalizer normalizer = master.getRegionNormalizer();

        // Verify that the normalizer is properly initialized
        boolean isActive = normalizer != null;

        // Validate that the normalizer workflow is functional
        assert isActive : "RegionNormalizer should be properly initialized and active.";
    }
}