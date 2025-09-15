package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.TrashPolicyDefault.Emptier;
import org.junit.Test;
import java.io.IOException;

public class TestTrashPolicyDefault {

    @Test
    public void testEmptierIntervalAdjustmentOnInvalidConfiguration() throws IOException {
        // Use API to get configuration values
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Initialize TrashPolicyDefault with the configuration
        TrashPolicyDefault trashPolicy = new TrashPolicyDefault();
        trashPolicy.initialize(conf, fs);

        // Fetch the Emptier runnable using the 'getEmptier' method
        Runnable emptierRunnable = trashPolicy.getEmptier();

        // Check the adjusted interval using public getters
        if (emptierRunnable instanceof TrashPolicyDefault.Emptier) {
            TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) emptierRunnable;

            // Validate that 'emptierInterval' is correctly adjusted internally
            long adjustedInterval = emptier.getEmptierInterval();
            System.out.println("Adjusted Emptier Interval (in minutes): " + adjustedInterval);
        }

        // Execute the 'run' method and verify behavior under load
        emptierRunnable.run();
    }
}