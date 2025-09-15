package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestFsTrashCheckpointIntervalConfig {

  @Test
  public void testCheckpointIntervalNotGreaterThanTrashInterval() {
    Configuration conf = new Configuration();
    conf.addResource("core-site.xml");

    float trashInterval = conf.getFloat(
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT);

    float checkpointInterval = conf.getFloat(
        CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT);

    assertTrue("fs.trash.checkpoint.interval must be <= fs.trash.interval",
        checkpointInterval <= trashInterval || checkpointInterval == 0);
  }
}