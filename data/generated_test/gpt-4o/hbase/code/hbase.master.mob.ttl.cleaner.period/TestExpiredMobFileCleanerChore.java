package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.master.ExpiredMobFileCleanerChore;
import org.apache.hadoop.hbase.master.HMaster;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

@Category({MasterTests.class, SmallTests.class})
public class TestExpiredMobFileCleanerChore {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestExpiredMobFileCleanerChore.class);

    @Test
    public void testExpiredMobFileCleanerChoreInitialization() throws Exception {
        // Prepare the test conditions
        // 1. Create a mock HMaster object.
        HMaster mockMaster = mock(HMaster.class);

        // 2. Configure the mock HMaster object to return a default configuration value for 'hbase.master.mob.ttl.cleaner.period'.
        Configuration mockConfiguration = mock(Configuration.class);
        when(mockMaster.getConfiguration()).thenReturn(mockConfiguration);
        when(mockConfiguration.getInt(eq("hbase.master.mob.ttl.cleaner.period"), anyInt()))
            .thenReturn(86400); // Default value is one day (in seconds).

        // Test code
        // 3. Instantiate ExpiredMobFileCleanerChore using the mock HMaster.
        ExpiredMobFileCleanerChore chore = new ExpiredMobFileCleanerChore(mockMaster);

        // 4. Assert that the 'period' and 'initialDelay' values match the expected configuration value.
        assertEquals("Expected chore period to be configured correctly", 
                     86400, chore.getPeriod());
        assertEquals("Expected chore initial delay to be configured correctly", 
                     86400L, chore.getInitialDelay()); // Correcting `assertEquals` to match the expected data types.

        // 5. Verify that the cleaner initialization logic, if any, matches the expected behavior.
        // Since `getCleaner()` does not exist, remove the erroneous call and adjust assertions accordingly. 
        // Ensure all other functionalities of the chore object meet the expectations.

        // Code after testing
        // Cleanup logic, if any (not needed for this mock-based test).
    }
}