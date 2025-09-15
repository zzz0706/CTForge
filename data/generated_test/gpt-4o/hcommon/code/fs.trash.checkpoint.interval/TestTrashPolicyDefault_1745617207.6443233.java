package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.TrashPolicyDefault;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import java.io.IOException;
import static org.junit.Assert.*;

/**
 * Test class to ensure configurations are effectively tested in TrashPolicyDefault.
 * Covers public methods to verify the behavior of configuration parsing and propagation.
 */
public class TestTrashPolicyDefault {

    // Get configuration values using the API
    // Prepare the input conditions for unit testing.
    @Test
    public void testEmptierIntervalAdjustmentOnInvalidConfiguration() throws IOException {
        // Step 1: Create a Configuration object with invalid or conflicting intervals
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f); // Trash deletion interval in minutes
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 20.0f); // Conflicting checkpoint interval in minutes

        // Step 2: Initialize FileSystem mock and TrashPolicyDefault
        FileSystem fileSystemMock = FileSystem.get(conf);
        TrashPolicyDefault trashPolicy = new TrashPolicyDefault();
        trashPolicy.initialize(conf, fileSystemMock);

        // Step 3: Fetch the Emptier instance using getEmptier()
        Runnable emptierRunnable = trashPolicy.getEmptier();

        // Validate adjusted emptierInterval using public getter
        assertTrue("Emptier should be an instance of TrashPolicyDefault.Emptier", emptierRunnable instanceof TrashPolicyDefault.Emptier);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) emptierRunnable;

        // Verify adjusted interval is consistent with deletion interval due to conflict
        long expectedEmptierInterval = 15; // Deletion interval
        assertEquals("Emptier interval was not adjusted correctly", expectedEmptierInterval, emptier.getEmptierInterval());

        // Step 4: Run the Emptier and validate trash processing behavior (not exhaustive, verifies no exception thrown)
        emptierRunnable.run();
    }
}