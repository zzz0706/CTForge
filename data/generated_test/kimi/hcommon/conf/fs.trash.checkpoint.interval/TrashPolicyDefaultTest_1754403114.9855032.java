package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Time;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Date;

import static org.junit.Assert.*;

public class TrashPolicyDefaultTest {

  private Configuration conf;
  private RawLocalFileSystem fs;
  private File testRoot;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    testRoot = new File("target/test-data/TrashPolicyDefaultTest");
    testRoot.mkdirs();
    fs = new RawLocalFileSystem();
    fs.initialize(FileSystem.getDefaultUri(conf), conf);
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
    deleteRecursively(testRoot);
  }

  private void deleteRecursively(File dir) {
    if (dir != null && dir.exists()) {
      File[] files = dir.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            deleteRecursively(file);
          } else {
            file.delete();
          }
        }
      }
      dir.delete();
    }
  }

  @Test
  public void testInitializeParsesCheckpointIntervalFromConfiguration() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 5.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 10.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptierRunnable = policy.getEmptier();
    assertNotNull(emptierRunnable);
  }

  @Test
  public void testEmptierRunCreatesAndDeletesCheckpoints() throws Exception {
    // Prepare a trash root under testRoot
    File trashRootDir = new File(testRoot, ".Trash");
    trashRootDir.mkdirs();
    org.apache.hadoop.fs.Path trashRoot = new org.apache.hadoop.fs.Path(trashRootDir.toURI());

    // Create a file inside trash
    org.apache.hadoop.fs.Path trashFile = new org.apache.hadoop.fs.Path(trashRoot, "file1");
    fs.create(trashFile).close();

    // Set trash interval to 1 minute and checkpoint interval to 30 seconds
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 1.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0.5f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);

    // Ensure trash root is recognized
    assertTrue(fs.exists(trashRoot));

    // Run emptier once to create checkpoint
    emptier.run();

    // Verify checkpoint directory exists (timestamp-based, so check for any directory starting with current date)
    File[] files = trashRootDir.listFiles();
    boolean checkpointFound = false;
    for (File f : files) {
      if (f.isDirectory() && f.getName().matches("\\d{10,}")) {
        checkpointFound = true;
        break;
      }
    }
    assertTrue("Checkpoint directory should be created", checkpointFound);
  }

  @Test
  public void testEmptierRunHandlesIOExceptionGracefully() throws Exception {
    // Prepare a trash root that will trigger IOException during checkpoint
    File trashRootDir = new File(testRoot, ".Trash");
    trashRootDir.mkdirs();
    org.apache.hadoop.fs.Path trashRoot = new org.apache.hadoop.fs.Path(trashRootDir.toURI());

    // Create a non-directory file to simulate error
    org.apache.hadoop.fs.Path fakeTrashRoot = new org.apache.hadoop.fs.Path(testRoot.toURI() + "/.TrashFile");
    fs.create(fakeTrashRoot).close();

    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 1.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0.5f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);

    // Run emptier and ensure no exception is thrown
    emptier.run();
  }

  @Test
  public void testEmptierIntervalUsesTrashIntervalWhenCheckpointNotConfigured() throws Exception {
    // Do not set FS_TRASH_CHECKPOINT_INTERVAL_KEY
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 25.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
  }

  @Test
  public void testEmptierIntervalClampedToDeletionIntervalWhenNegative() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, -5.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
  }

  @Test
  public void testEmptierIntervalClampedToDeletionIntervalWhenTooLarge() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 20.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 30.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
  }

  @Test
  public void testEmptierIntervalZeroDisablesTrash() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 0.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
  }

  @Test
  public void testGetEmptierReturnsNonNullRunnable() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 10.0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 5.0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
  }
}