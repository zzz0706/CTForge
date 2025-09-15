package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import java.io.IOException;

public class TestTrashPolicyDefault {

    // Get configuration values using the API
    // Prepare the input conditions for unit testing
    @Test
    public void testEmptierIntervalAdjustmentOnInvalidConfiguration() throws IOException {
        // Step 1: Create a Configuration object with invalid or conflicting intervals
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f); // Trash deletion interval in minutes
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 20.0f); // Conflicting checkpoint interval in minutes

        // Step 2: Initialize FileSystem mock and TrashPolicyDefault
        FileSystem fs = FileSystem.get(conf);
        TrashPolicyDefault trashPolicy = new TrashPolicyDefault();
        trashPolicy.initialize(conf, fs);

        // Step 3: Fetch the Emptier instance using getEmptier()
        Runnable emptierRunnable = trashPolicy.getEmptier();

        // Validate adjusted emptierInterval using public getter
        if (emptierRunnable instanceof TrashPolicyDefault.Emptier) {
            TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) emptierRunnable;

            // Verify emptierInterval adjustment
            long adjustedInterval = emptier.getEmptierInterval();
            System.out.println("Adjusted Emptier Interval (in minutes): " + adjustedInterval);

            // Validate that the adjusted interval matches the expected behavior
            assert adjustedInterval == 15 : "Emptier interval was not adjusted correctly!";
        }

        // Step 4: Execute the 'run' method and validate behavior
        emptierRunnable.run();
    }
}