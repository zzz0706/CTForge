package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestViewfsRenameStrategyConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  @Test
  public void testValidRenameStrategySAME_MOUNTPOINT() {
    // 1. Obtain configuration value
    conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "SAME_MOUNTPOINT");
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    // 2. Validate enumeration value
    assertTrue("SAME_MOUNTPOINT should be accepted",
               isValidRenameStrategy(strategy));
  }

  @Test
  public void testValidRenameStrategySAME_TARGET_URI_ACROSS_MOUNTPOINT() {
    conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
             "SAME_TARGET_URI_ACROSS_MOUNTPOINT");
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    assertTrue("SAME_TARGET_URI_ACROSS_MOUNTPOINT should be accepted",
               isValidRenameStrategy(strategy));
  }

  @Test
  public void testValidRenameStrategySAME_FILESYSTEM_ACROSS_MOUNTPOINT() {
    conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
             "SAME_FILESYSTEM_ACROSS_MOUNTPOINT");
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    assertTrue("SAME_FILESYSTEM_ACROSS_MOUNTPOINT should be accepted",
               isValidRenameStrategy(strategy));
  }

  @Test
  public void testInvalidRenameStrategy() {
    conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "INVALID_STRATEGY");
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    assertFalse("Invalid strategy should be rejected",
                isValidRenameStrategy(strategy));
  }

  @Test
  public void testEmptyRenameStrategy() {
    conf.set(Constants.CONFIG_VIEWFS_RENAME_STRATEGY, "");
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    assertTrue("Empty strategy should be accepted as default",
               isValidRenameStrategy(strategy));
  }

  @Test
  public void testNullRenameStrategy() {
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY);

    // null falls back to default
    assertTrue("null defaults to SAME_MOUNTPOINT",
               isValidRenameStrategy(strategy));
  }

  private boolean isValidRenameStrategy(String value) {
    if (value == null || value.isEmpty()) {
      return true; // null/empty means default SAME_MOUNTPOINT
    }
    try {
      ViewFileSystem.RenameStrategy.valueOf(value);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    }
  }
}