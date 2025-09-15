package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TrashPolicyDefaultTest {

    @Test
    public void verifyCheckpointIntervalEqualsTrashIntervalWhenZero() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0f);

        float trashIntervalMinutes = conf.getFloat(
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
                CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT);
        long expectedEmptierInterval = (long)(trashIntervalMinutes);

        FileSystem mockFs = new RawLocalFileSystem();
        mockFs.initialize(FileSystem.getDefaultUri(conf), conf);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        assertEquals(expectedEmptierInterval, emptier.getEmptierInterval());
    }

    @Test
    public void verifyCheckpointIntervalClampedToDeletionIntervalWhenTooLarge() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 30f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 60f);

        FileSystem mockFs = new RawLocalFileSystem();
        mockFs.initialize(FileSystem.getDefaultUri(conf), conf);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        assertEquals(30L, emptier.getEmptierInterval());
    }

    @Test
    public void verifyCheckpointIntervalClampedToDeletionIntervalWhenNegative() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 20f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, -5f);

        FileSystem mockFs = new RawLocalFileSystem();
        mockFs.initialize(FileSystem.getDefaultUri(conf), conf);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        assertEquals(20L, emptier.getEmptierInterval());
    }

    @Test
    public void verifyCheckpointIntervalAcceptedWhenValid() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 100f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 50f);

        FileSystem mockFs = new RawLocalFileSystem();
        mockFs.initialize(FileSystem.getDefaultUri(conf), conf);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        assertEquals(50L, emptier.getEmptierInterval());
    }

    @Test
    public void verifyCheckpointIntervalZeroWithCustomTrashInterval() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0f);

        FileSystem mockFs = new RawLocalFileSystem();
        mockFs.initialize(FileSystem.getDefaultUri(conf), conf);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);
        TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

        assertEquals(15L, emptier.getEmptierInterval());
    }
}