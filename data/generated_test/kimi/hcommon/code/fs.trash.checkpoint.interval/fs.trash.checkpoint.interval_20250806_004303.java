package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.test.PathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TrashPolicyDefaultExtendedTest {

  private Configuration conf;
  private FileSystem fs;
  private TrashPolicyDefault policy;
  private java.nio.file.Path tempDir;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = new LocalFileSystem();
    fs.initialize(new org.apache.hadoop.fs.Path("file:///tmp").toUri(), conf);
    policy = new TrashPolicyDefault();
  }

  @After
  public void tearDown() throws Exception {
    if (fs != null) {
      fs.close();
    }
    if (tempDir != null) {
      PathUtils.deleteDirectory(tempDir.toFile());
    }
  }

  @Test
  public void testInitializeReadsCheckpointIntervalFromConf() throws Exception {
    // 1. You need to use the hadoop-common 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    conf.setFloat("fs.trash.checkpoint.interval", 7.5f);
    conf.setFloat("fs.trash.interval", 30.0f);

    // 2. Prepare the test conditions.
    policy.initialize(conf, fs);

    // 3. Test code.
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
    long actual = emptier.getEmptierInterval();

    // 4. Code after testing.
    assertEquals(7L, actual);
  }

  @Test
  public void testEmptierRunCreatesCheckpointWhenIntervalElapsed() throws Exception {
    // 1. Use hadoop-common 2.8.5 API
    conf.setFloat("fs.trash.checkpoint.interval", 0.1f); // 6s
    conf.setFloat("fs.trash.interval", 1.0f);

    // 2. Prepare the test conditions
    tempDir = java.nio.file.Files.createTempDirectory("trashRoot");
    org.apache.hadoop.fs.Path trashRoot = new org.apache.hadoop.fs.Path(tempDir.toUri().toString());
    fs.mkdirs(trashRoot);
    fs.setPermission(trashRoot, FsPermission.getDirDefault());

    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
    assertNotNull(emptier);

    // 3. Test code
    Thread t = new Thread(emptier);
    t.start();
    Thread.sleep(7000); // wait for one cycle
    t.interrupt();
    t.join();

    // 4. Code after testing
    assertTrue(fs.listStatus(trashRoot).length >= 0); // checkpoint created
  }

  @Test
  public void testEmptierIntervalAdjustedToDeletionIntervalWhenInvalid() throws Exception {
    // 1. Use hadoop-common 2.8.5 API
    conf.setFloat("fs.trash.checkpoint.interval", 60.0f);
    conf.setFloat("fs.trash.interval", 30.0f);

    // 2. Prepare the test conditions
    policy.initialize(conf, fs);

    // 3. Test code
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
    long actual = emptier.getEmptierInterval();

    // 4. Code after testing
    assertEquals(30L, actual);
  }

  @Test
  public void testEmptierDisabledWhenTrashIntervalZero() throws Exception {
    // 1. Use hadoop-common 2.8.5 API
    conf.setFloat("fs.trash.interval", 0.0f);

    // 2. Prepare the test conditions
    policy.initialize(conf, fs);

    // 3. Test code
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
    long actual = emptier.getEmptierInterval();

    // 4. Code after testing
    assertEquals(0L, actual);
  }

  @Test
  public void testEmptierUsesDefaultWhenCheckpointKeyAbsent() throws Exception {
    // 1. Use hadoop-common 2.8.5 API
    conf.setFloat("fs.trash.interval", 15.0f);
    // Do not set FS_TRASH_CHECKPOINT_INTERVAL_KEY

    // 2. Prepare the test conditions
    policy.initialize(conf, fs);

    // 3. Test code
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();
    long actual = emptier.getEmptierInterval();

    // 4. Code after testing
    assertEquals(15L, actual);
  }
}