package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class TrashPolicyDefaultTest {

    @Test
    public void verifyCheckpointIntervalEqualsTrashIntervalWhenZero() throws Exception {
        // 1. Configuration as input
        Configuration conf = new Configuration();

        // 2. Dynamic expected value calculation
        float trashIntervalMinutes = conf.getFloat(
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT);
        long expectedEmptierInterval = (long)(trashIntervalMinutes);

        // 3. Mock external dependencies
        FileSystem mockFs = Mockito.mock(FileSystem.class);

        // 4. Invoke method under test
        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        // 5. Assertions
        assertEquals(expectedEmptierInterval, emptier.getEmptierInterval());
    }
}