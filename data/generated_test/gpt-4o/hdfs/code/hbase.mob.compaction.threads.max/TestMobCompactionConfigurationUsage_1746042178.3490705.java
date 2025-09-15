package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.MobCompactionChore;
import org.apache.hadoop.hbase.master.MasterMobCompactionThread;
import org.apache.hadoop.hbase.mob.MobUtils;
import org.apache.hadoop.hbase.mob.MobConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;

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
        // Prepare mocked configuration object
        mockConfig = mock(Configuration.class);

        // Set mock behavior to retrieve configuration properties
        when(mockConfig.getInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX)).thenReturn(4);
        when(mockConfig.getInt("hbase.mob.compaction.chore.period", 600)).thenReturn(600);

        // Prepare mocked HMaster object
        mockMaster = mock(HMaster.class);
        when(mockMaster.getConfiguration()).thenReturn(mockConfig);
    }

    @Test
    public void testCreateMobCompactorThreadPool() {
        // Test MobUtils.createMobCompactorThreadPool(Configuration conf)
        ExecutorService pool = MobUtils.createMobCompactorThreadPool(mockConfig);

        assertNotNull("Mob compactor thread pool should be created successfully", pool);
        assertTrue("Thread pool should be an instance of ThreadPoolExecutor", pool instanceof java.util.concurrent.ThreadPoolExecutor);

        // Verify mock configuration interaction
        verify(mockConfig).getInt(MobConstants.MOB_COMPACTION_THREADS_MAX, MobConstants.DEFAULT_MOB_COMPACTION_THREADS_MAX);
    }

    @Test
    public void testMasterMobCompactionThreadInitialization() {
        // Test the initialization of the MasterMobCompactionThread
        MasterMobCompactionThread masterThread = new MasterMobCompactionThread(mockMaster);

        assertNotNull("MasterMobCompactionThread should be properly initialized", masterThread);

        // Verify configuration interaction
        verify(mockMaster, atLeastOnce()).getConfiguration();
    }

    @Test
    public void testMobCompactionChore() {
        // Test the initialization and behavior of MobCompactionChore
        Configuration configuration = mockMaster.getConfiguration();
        int period = configuration.getInt("hbase.mob.compaction.chore.period", 600);

        MobCompactionChore chore = new MobCompactionChore(mockMaster, period);

        assertNotNull("MobCompactionChore should be properly initialized", chore);

        // Ensure the thread pool is created correctly
        ExecutorService pool = MobUtils.createMobCompactorThreadPool(mockMaster.getConfiguration());
        assertNotNull("Mob compactor thread pool should be created properly", pool);

        // Verify configuration interaction
        verify(mockMaster, atLeastOnce()).getConfiguration();
        verify(mockConfig).getInt("hbase.mob.compaction.chore.period", 600);
    }
}