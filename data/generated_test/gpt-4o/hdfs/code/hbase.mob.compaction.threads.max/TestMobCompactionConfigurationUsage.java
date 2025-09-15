package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.MobCompactionChore;
import org.apache.hadoop.hbase.master.MasterMobCompactionThread;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.MobConstants;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.ExecutorService;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@Category({MasterTests.class, SmallTests.class})
public class TestMobCompactionConfigurationUsage {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobCompactionConfigurationUsage.class);

    private Configuration mockConfig;
    private HMaster mockMaster;

    @Before
    public void setUp() {
        // Setup mock Configuration object and HMaster
        mockConfig = mock(Configuration.class);

        // Define mock behavior for retrieving configurations
        when(mockConfig.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX))
                .thenReturn(4); // Mock value for max threads
        when(mockConfig.getInt("hbase.mob.compaction.chore.period", 600))
                .thenReturn(600); // Mock value for chore period

        mockMaster = mock(HMaster.class);
        when(mockMaster.getConfiguration()).thenReturn(mockConfig);
    }

    @Test
    public void testCreateMobCompactorThreadPool() {
        // Test the createMobCompactorThreadPool method in MobUtils
        ExecutorService pool = MobUtils.createMobCompactorThreadPool(mockConfig);

        assertNotNull("ExecutorService should be created successfully", pool);
        assertTrue("Pool should be an instance of ThreadPoolExecutor", pool instanceof java.util.concurrent.ThreadPoolExecutor);

        // Verify configuration interaction
        verify(mockConfig).getInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);
    }

    @Test
    public void testMasterMobCompactionThreadInitialization() {
        // Test the initialization of MasterMobCompactionThread
        MasterMobCompactionThread masterThread = new MasterMobCompactionThread(mockMaster);

        assertNotNull("MasterMobCompactionThread should be initialized", masterThread);

        // Verify configuration interaction
        verify(mockMaster, atLeastOnce()).getConfiguration();
    }

    @Test
    public void testMobCompactionChore() throws Exception {
        // Test the initialization and usage of MobCompactionChore
        Configuration conf = mockMaster.getConfiguration();
        int period = conf.getInt("hbase.mob.compaction.chore.period", 600);
        MobCompactionChore chore = new MobCompactionChore(mockMaster, period);

        assertNotNull("MobCompactionChore should be initialized", chore);

        // Verify that the thread pool is created correctly
        ExecutorService pool = MobUtils.createMobCompactorThreadPool(mockMaster.getConfiguration());
        assertNotNull("Thread pool should be initialized", pool);

        // Verify configuration interaction
        verify(mockMaster, atLeastOnce()).getConfiguration();
        verify(mockConfig).getInt("hbase.mob.compaction.chore.period", 600);
    }
}