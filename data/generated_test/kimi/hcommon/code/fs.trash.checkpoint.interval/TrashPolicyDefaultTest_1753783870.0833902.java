package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class TrashPolicyDefaultTest {

    @Test
    public void verifyCheckpointIntervalIsUsedDirectlyWhenValid() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Dynamic Expected Value Calculation
        float trashInterval = conf.getFloat(
            CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
            CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT);
        float checkpointInterval = conf.getFloat(
            CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY,
            CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT);
        // Override to match prerequisites
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 5.0f);
        trashInterval = 15.0f;
        checkpointInterval = 5.0f;
        long expectedCheckpointMinutes = (long) checkpointInterval;

        // 3. Mock/Stub External Dependencies
        FileSystem mockFs = Mockito.mock(FileSystem.class);

        // 4. Invoke the Method Under Test
        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        // 5. Assertions and Verification
        assertEquals(expectedCheckpointMinutes, actualCheckpointMinutes);
    }
}