package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.Before;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import java.lang.reflect.Field;
import java.util.concurrent.ThreadPoolExecutor;

@Category({MasterTests.class, MediumTests.class})
public class TestMasterMobCompactionThread {
    
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMasterMobCompactionThread.class);

    private HBaseTestingUtility utility;

    @Before
    public void setup() throws Exception {
        // Set up HBase testing utility
        utility = new HBaseTestingUtility();
        utility.startMiniCluster();
    }

    @Test
    public void test_MasterMobCompactionThread_initialization() throws Exception {
        // 1. Prepare the test conditions: Get the HBase configuration instance.
        // HMaster is part of the HBase master environment. Create a mock HMaster object
        HMaster mockMaster = utility.getMiniHBaseCluster().getMaster();

        // 2. Test code: Create instance and verify the mobCompactorPool initialization.
        MasterMobCompactionThread masterMobThread = new MasterMobCompactionThread(mockMaster);

        Field mobCompactorPoolField = MasterMobCompactionThread.class.getDeclaredField("mobCompactorPool");
        mobCompactorPoolField.setAccessible(true);
        ThreadPoolExecutor mobCompactorPool = (ThreadPoolExecutor) mobCompactorPoolField.get(masterMobThread);

        // Validate thread pool properties
        int corePoolSize = mobCompactorPool.getCorePoolSize();
        int maxPoolSize = mobCompactorPool.getMaximumPoolSize();
        
        // Assert that the thread pool configuration aligns with expectations.
        // Note: Avoid hardcoding values. Values should be checked dynamically.
        assert corePoolSize > 0; // Core pool size should be positive.
        assert maxPoolSize >= corePoolSize; // Max pool size should be greater than or equal to core pool size.

        // Validate that the thread pool was created using the provided configuration.
        assert mobCompactorPool.getQueue() instanceof java.util.concurrent.SynchronousQueue;

        mobCompactorPool.shutdown();
    }
}