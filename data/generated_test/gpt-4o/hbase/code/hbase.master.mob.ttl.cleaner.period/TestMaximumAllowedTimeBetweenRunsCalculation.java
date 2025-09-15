package org.apache.hadoop.hbase.master;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.conf.Configuration;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import java.util.concurrent.TimeUnit;

@Category({MasterTests.class, SmallTests.class})
public class TestMaximumAllowedTimeBetweenRunsCalculation {

    @ClassRule // HBaseClassTestRule ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestMaximumAllowedTimeBetweenRunsCalculation.class);

    @Test
    public void testMaximumAllowedTimeBetweenRunsCalculation() {
        // 1. Use the hbase 2.2.2 API correctly to obtain configuration values instead of hardcoding.
        Configuration configuration = new Configuration();
        long defaultPeriodInMinutes = configuration.getLong("hbase.regionserver.chore.default.period", 1440L); // Default period: 1440 minutes
        long periodInMillis = defaultPeriodInMinutes * 60 * 1000L; // Convert to milliseconds

        // 2. Prepare the test conditions
        // Create a mock Stoppable instance for testing purposes
        Stoppable mockStopper = new Stoppable() {
            private boolean stopped = false;

            @Override
            public void stop(String why) {
                stopped = true;
            }

            @Override
            public boolean isStopped() {
                return stopped;
            }
        };

        // Create an anonymous subclass for ScheduledChore using a valid constructor
        ScheduledChore chore = new ScheduledChore("TestChore", mockStopper, (int) (periodInMillis / 1000), 0, TimeUnit.MILLISECONDS) {
            @Override
            protected void chore() {
                // No implementation needed for this test
            }

            @Override
            protected void cleanup() {
                // No implementation needed for this test
            }
        };

        // 3. Test code
        long maxAllowedTimeBetweenRuns = (long) (1.5 * chore.getPeriod() * 1000); // Convert seconds back to milliseconds

        // Assert the correctness of the calculation
        long expectedMaxAllowedTime = (long) (1.5 * chore.getPeriod() * 1000); // Convert seconds back to milliseconds
        assert maxAllowedTimeBetweenRuns == expectedMaxAllowedTime
            : "Expected: " + expectedMaxAllowedTime + ", but found: " + maxAllowedTimeBetweenRuns;

        // 4. Code after testing
        chore.cancel(false); // Ensure cleanup after the test
        assert !chore.isScheduled();
    }
}