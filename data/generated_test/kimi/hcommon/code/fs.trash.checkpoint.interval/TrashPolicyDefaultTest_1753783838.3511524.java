package org.apache.hadoop.fs;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({FileSystem.class})
public class TrashPolicyDefaultTest {

    @Test
    public void verifyCheckpointIntervalIsCappedByTrashInterval() throws Exception {
        // 1. Configuration as Input
        Configuration conf = new Configuration();

        // 2. Dynamic Expected Value Calculation
        long expectedDeletionInterval = (long) (conf.getFloat(
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT) * 60000);
        long expectedCheckpointInterval = expectedDeletionInterval / 60000;

        // 3. Mock/Stub External Dependencies
        FileSystem mockFs = mock(FileSystem.class);
        PowerMockito.mockStatic(FileSystem.class);
        when(FileSystem.newInstance(conf)).thenReturn(mockFs);

        // 4. Invoke the Method Under Test
        TrashPolicyDefault trashPolicy = new TrashPolicyDefault();
        trashPolicy.initialize(conf, mockFs);
        Runnable emptier = trashPolicy.getEmptier();
        long actualInterval = ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval();

        // 5. Assertions and Verification
        assertEquals(expectedCheckpointInterval, actualInterval);
    }
}