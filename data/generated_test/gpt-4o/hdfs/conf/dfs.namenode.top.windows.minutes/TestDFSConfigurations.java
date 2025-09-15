package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;
import org.junit.Assert;

import java.util.concurrent.TimeUnit;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;

public class TestDFSConfigurations {

    /**
     * Test method to validate the correctness of the configuration
     * `dfs.namenode.top.windows.minutes` based on constraints and dependencies.
     */
    @Test
    public void testNNTOPWindowsMinutesConfiguration() {
        // Step 1: Load configuration from file or create Configuration object
        Configuration conf = new Configuration();
        
        // Step 2: Read the configuration value
        String[] periodsStr = conf.getTrimmedStrings(
                DFSConfigKeys.NNTOP_WINDOWS_MINUTES_KEY,
                DFSConfigKeys.NNTOP_WINDOWS_MINUTES_DEFAULT // Default value: "1,5,25"
        );

        // Step 3: Parse the configuration values and validate constraints
        try {
            int[] nntopReportingPeriodsMs = new int[periodsStr.length];
            for (int i = 0; i < periodsStr.length; i++) {
                // Attempt to parse the value into an integer
                int periodMinutes = Integer.parseInt(periodsStr[i]);

                // Convert to milliseconds and store
                nntopReportingPeriodsMs[i] = Ints.checkedCast(
                    TimeUnit.MINUTES.toMillis(periodMinutes)
                );

                // Check if the value satisfies the constraint (minimum reporting period >= 1 minute)
                Preconditions.checkArgument(
                    nntopReportingPeriodsMs[i] >= TimeUnit.MINUTES.toMillis(1),
                    "Minimum reporting period is 1 minute!"
                );
            }

            // Assert all parsed and validated values are correct (if required for the test)
            Assert.assertEquals("Unexpected number of reporting periods.", 3, nntopReportingPeriodsMs.length);
            Assert.assertTrue("All periods must be >= 1 minute.", nntopReportingPeriodsMs[0] >= TimeUnit.MINUTES.toMillis(1));
            Assert.assertTrue("Validation passed successfully.", true);

        } catch (NumberFormatException e) {
            // Fail the test if invalid numeric values are provided
            Assert.fail("Configuration contains invalid non-numeric values: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            // Fail the test if constraint violations occur
            Assert.fail("Configuration violated constraints: " + e.getMessage());
        } catch (Exception e) {
            // Fail the test for any other unexpected exceptions
            Assert.fail("Unexpected exception encountered: " + e.getMessage());
        }
    }
}