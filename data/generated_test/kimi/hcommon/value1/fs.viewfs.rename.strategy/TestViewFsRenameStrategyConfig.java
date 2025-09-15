package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class TestViewFsRenameStrategyConfig {

  @Test
  public void testValidRenameStrategyValue() {
    Configuration conf = new Configuration();
    // Do NOT set the value in code; rely on the loaded configuration files.
    String strategy = conf.get(Constants.CONFIG_VIEWFS_RENAME_STRATEGY,
                               ViewFileSystem.RenameStrategy.SAME_MOUNTPOINT.toString());

    boolean valid = false;
    for (ViewFileSystem.RenameStrategy allowed : ViewFileSystem.RenameStrategy.values()) {
      if (allowed.name().equals(strategy)) {
        valid = true;
        break;
      }
    }
    assertTrue("Invalid fs.viewfs.rename.strategy value: " + strategy, valid);
  }
}