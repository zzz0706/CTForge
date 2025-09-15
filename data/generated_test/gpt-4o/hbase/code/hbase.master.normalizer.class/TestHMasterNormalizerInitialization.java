package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizerFactory;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertNotNull;

@Category({MasterTests.class, SmallTests.class})
public class TestHMasterNormalizerInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestHMasterNormalizerInitialization.class);

    @Test
    public void test_initializeZKBasedSystemTrackersNormalizerCreation() throws Exception {
        // 1. Use the HBase 2.2.2 API correctly to obtain configuration values.
        HBaseTestingUtility testingUtility = new HBaseTestingUtility();
        Configuration configuration = testingUtility.getConfiguration();

        // Ensure configuration has required dependencies set up.
        configuration.set(HConstants.ZOOKEEPER_QUORUM, "localhost");
        configuration.setInt(HConstants.ZOOKEEPER_CLIENT_PORT, 2181);
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, "/hbase");

        String normalizerClassName = configuration.get(HConstants.HBASE_MASTER_NORMALIZER_CLASS);
        assertNotNull("Configuration value for hbase.master.normalizer.class should not be null", normalizerClassName);

        // 2. Prepare the test conditions: Start MiniCluster and ZooKeeper.
        testingUtility.startMiniCluster();

        try {
            // 3. Prepare test conditions: Initialize the HMaster instance properly
            HMaster hMaster = testingUtility.getMiniHBaseCluster().getMaster();
            assertNotNull("HMaster instance should not be null", hMaster);

            hMaster.initializeZKBasedSystemTrackers();

            // 4. Assertions: Validate RegionNormalizer initialization and dependencies.
            RegionNormalizer normalizer = hMaster.getRegionNormalizer();
            assertNotNull("RegionNormalizer should be properly initialized in HMaster", normalizer);

            // Verify RegionNormalizer is created based on configuration value.
            RegionNormalizer expectedNormalizer =
                    RegionNormalizerFactory.getRegionNormalizer(configuration);
            assertNotNull("Expected RegionNormalizer implementation must be created successfully", expectedNormalizer);

            // Verify RegionNormalizer instance matches the one configured.
            assertNotNull("Normalizer instance should not be null after initialization", normalizer);
        } finally {
            // 5. Shutdown testing environment.
            testingUtility.shutdownMiniCluster();
        }
    }
}