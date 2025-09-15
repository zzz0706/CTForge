package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class TrashPolicyDefaultTest {

    @Test
    public void verifyCheckpointIntervalIsUsedDirectlyWhenValid() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 5.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        assertEquals(5L, actualCheckpointMinutes);
    }

    @Test
    public void verifyCheckpointIntervalIsResetToDeletionIntervalWhenLarger() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 10.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 20.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        assertEquals(10L, actualCheckpointMinutes);
    }

    @Test
    public void verifyCheckpointIntervalIsResetToDeletionIntervalWhenZero() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 12.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        assertEquals(12L, actualCheckpointMinutes);
    }

    @Test
    public void verifyCheckpointIntervalIsResetToDeletionIntervalWhenNegative() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 7.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, -3.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        assertEquals(7L, actualCheckpointMinutes);
    }

    @Test
    public void verifyCheckpointIntervalEqualsDeletionIntervalWhenNotConfigured() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 9.0f);
        // Do not set checkpoint interval, should default to 0 and then use deletion interval

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
        long actualCheckpointMinutes = emptier.getEmptierInterval();

        assertEquals(9L, actualCheckpointMinutes);
    }
}