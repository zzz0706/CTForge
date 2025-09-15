package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.FileStatus;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class TrashPolicyDefaultTest {

    @Test
    public void testEmptierRunWithValidCheckpointInterval() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 5.0f);

        // Prepare a mock FileSystem
        FileSystem mockFs = Mockito.mock(FileSystem.class);
        Path trashRootPath = new Path("/user/test/.Trash");
        FileStatus mockStatus = Mockito.mock(FileStatus.class);
        when(mockStatus.isDirectory()).thenReturn(true);
        when(mockStatus.getPath()).thenReturn(trashRootPath);
        Collection<FileStatus> trashRoots = new ArrayList<>();
        trashRoots.add(mockStatus);
        when(mockFs.getTrashRoots(true)).thenReturn(trashRoots);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);

        // Obtain the Emptier runnable
        Runnable emptier = policy.getEmptier();
        assertEquals(5L, ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval());

        // Run the emptier once (interrupt after one cycle)
        Thread emptierThread = new Thread(emptier);
        emptierThread.start();
        Thread.sleep(100); // Allow run() to enter the loop
        emptierThread.interrupt();
        emptierThread.join(1000);
    }

    @Test
    public void testEmptierRunWithTrashDisabled() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 0.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);

        Runnable emptier = policy.getEmptier();
        assertEquals(0L, ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval());

        // When interval is 0, run() should return immediately
        Thread emptierThread = new Thread(emptier);
        emptierThread.start();
        emptierThread.join(1000);
        assertEquals(false, emptierThread.isAlive());
    }

    @Test
    public void testEmptierIntervalValidationNegative() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 7.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, -3.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);

        Runnable emptier = policy.getEmptier();
        assertEquals(7L, ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval());
    }

    @Test
    public void testEmptierIntervalValidationZero() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 12.0f);
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0.0f);

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);

        Runnable emptier = policy.getEmptier();
        assertEquals(12L, ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval());
    }

    @Test
    public void testEmptierIntervalValidationNotConfigured() throws Exception {
        Configuration conf = new Configuration();
        conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 9.0f);
        // Do not set checkpoint interval, should default to 0 and then use deletion interval

        FileSystem mockFs = Mockito.mock(FileSystem.class);

        TrashPolicyDefault policy = new TrashPolicyDefault();
        policy.initialize(conf, mockFs);

        Runnable emptier = policy.getEmptier();
        assertEquals(9L, ((TrashPolicyDefault.Emptier) emptier).getEmptierInterval());
    }
}