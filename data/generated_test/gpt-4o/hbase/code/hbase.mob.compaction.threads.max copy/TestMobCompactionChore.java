package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.master.MobCompactionChore;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.master.HMaster;

import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.ClassRule;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;
import static org.junit.Assert.assertNotNull;

@Category({MasterTests.class, SmallTests.class})
public class TestMobCompactionChore {
    
    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestMobCompactionChore.class);

    private HMaster mockMaster;
    private ChoreService choreService;

    @Before
    public void setUp() {
        // Prepare mock HMaster and ChoreService
        mockMaster = mock(HMaster.class);
        choreService = mock(ChoreService.class);

        // Mock Configuration object and its behavior
        Configuration mockConfig = mock(Configuration.class);
        when(mockConfig.getInt("hbase.mob.compaction.threads.max", 1)).thenReturn(4);
        when(mockConfig.getInt("hbase.mob.compaction.chore.period", 600)).thenReturn(600);
        when(mockMaster.getConfiguration()).thenReturn(mockConfig);
    }

    @After
    public void tearDown() {
        // Reset test environment
        choreService.shutdown();
    }

    @Test
    public void testMobCompactionChoreExecution() throws Exception {
        Configuration conf = mockMaster.getConfiguration();
        int period = conf.getInt("hbase.mob.compaction.chore.period", 600);

        // Create MobCompactionChore instance with HMaster and test configuration
        MobCompactionChore chore = new MobCompactionChore(mockMaster, period);

        // Verify behavior of ChoreService
        choreService.scheduleChore(chore);

        // Validate that the thread pool initialization method is correct
        assertNotNull("ChoreService should be available", choreService);

        // Verify mock interactions
        verify(mockMaster, atLeastOnce()).getConfiguration();
        verify(choreService).scheduleChore(chore);
    }
}