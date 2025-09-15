package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsTrashCheckpointIntervalConfig {

  @Test
  public void testCheckpointIntervalNotGreaterThanTrashInterval() {
    Configuration conf = new Configuration();
    // Do NOT set any values programmatically – rely on loaded configuration files
    float checkpointMinutes = conf.getFloat(
        CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_TRASH_CHECKPOINT_INTERVAL_DEFAULT);

    float trashMinutes = conf.getFloat(
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_KEY,
        CommonConfigurationKeysPublic.FS_TRASH_INTERVAL_DEFAULT);

    // fs.trash.checkpoint.interval must be <= fs.trash.interval
    assertTrue(
        "fs.trash.checkpoint.interval (" + checkpointMinutes +
        ") must be <= fs.trash.interval (" + trashMinutes + ")",
        checkpointMinutes <= trashMinutes);
  }
}