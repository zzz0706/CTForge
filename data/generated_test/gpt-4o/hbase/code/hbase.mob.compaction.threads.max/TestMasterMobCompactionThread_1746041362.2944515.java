package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.master.HMaster;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ThreadPoolExecutor;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterMobCompactionThread {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterMobCompactionThread.class);

    private HBaseTestingUtility utility;
    private Configuration conf;

    @Before
    public void setup() throws Exception {
        // Set up HBase testing utility
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
        conf = utility.getConfiguration();

        // Ensure the configuration for mob compaction threads is set using HBase API
        conf.setInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);
    }

    @Test
    public void test_MasterMobCompactionThread_initialization() throws Exception {
        // 1. Prepare the test conditions: Create an HMaster instance from the mini cluster
        HMaster master = utility.getMiniHBaseCluster().getMaster();

        // 2. Instantiate MasterMobCompactionThread and verify mobCompactorPool initialization
        MasterMobCompactionThread masterMobThread = new MasterMobCompactionThread(master);

        // Create mob compactor thread pool using helper utility MobUtils
        ThreadPoolExecutor mobCompactorPool = (ThreadPoolExecutor) MobUtils.createMobCompactorThreadPool(conf);

        // 3. Validate thread pool properties dynamically based on configuration
        int expectedMaxPoolSize = conf.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);

        // Check if the pool's max size matches the configuration
        assert mobCompactorPool.getMaximumPoolSize() == expectedMaxPoolSize 
            : "MaxPoolSize mismatch: Expected " + expectedMaxPoolSize + " but got " + mobCompactorPool.getMaximumPoolSize();

        // Additional assertions can be made to verify the behavior of the thread pool
        assert mobCompactorPool.getCorePoolSize() > 0 : "CorePoolSize should be greater than 0";

        // 4. Code after testing: Clean up mini cluster
        utility.shutdownMiniCluster();
    }
}