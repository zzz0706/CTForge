package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TrashPolicyDefaultTest {

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws IOException {
    conf = new Configuration();
    fs = new RawLocalFileSystem();
    fs.initialize(FileSystem.getDefaultUri(conf), conf);
  }

  @After
  public void tearDown() throws IOException {
    if (fs != null) {
      fs.close();
    }
  }

  @Test
  public void verifyCheckpointIntervalEqualsTrashIntervalWhenZero() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 10f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    assertEquals(10L, emptier.getEmptierInterval());
  }

  @Test
  public void verifyCheckpointIntervalClampedToDeletionIntervalWhenTooLarge() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 30f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 60f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    assertEquals(30L, emptier.getEmptierInterval());
  }

  @Test
  public void verifyCheckpointIntervalClampedToDeletionIntervalWhenNegative() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 20f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, -5f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    assertEquals(20L, emptier.getEmptierInterval());
  }

  @Test
  public void verifyCheckpointIntervalAcceptedWhenValid() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 100f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 50f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    assertEquals(50L, emptier.getEmptierInterval());
  }

  @Test
  public void verifyCheckpointIntervalZeroWithCustomTrashInterval() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 0f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    assertEquals(15L, emptier.getEmptierInterval());
  }

  @Test
  public void verifyGetEmptierReturnsNonNullRunnable() throws Exception {
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 15f);
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY, 10f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);

    Runnable emptier = policy.getEmptier();
    assertNotNull(emptier);
    assertEquals(TrashPolicyDefault.Emptier.class, emptier.getClass());
  }

  @Test
  public void verifyEmptierIntervalUsesDefaultWhenNotConfigured() throws Exception {
    // Do not set FS_TRASH_CHECKPOINT_INTERVAL_KEY; rely on default
    conf.setFloat(CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY, 30f);

    TrashPolicyDefault policy = new TrashPolicyDefault();
    policy.initialize(conf, fs);
    TrashPolicyDefault.Emptier emptier = (TrashPolicyDefault.Emptier) policy.getEmptier();

    // FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT is 0, so when it is unset, the policy
    // uses the trash interval (30) instead.
    assertEquals(30L, emptier.getEmptierInterval());
  }
}