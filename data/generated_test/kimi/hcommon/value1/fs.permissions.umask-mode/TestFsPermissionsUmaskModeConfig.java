package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestFsPermissionsUmaskModeConfig {

  @Test
  public void testUmaskModeValid() {
    Configuration conf = new Configuration(false);
    // 1. Load configuration from core-site.xml / hdfs-site.xml
    conf.addResource("core-site.xml");
    conf.addResource("hdfs-site.xml");

    // 2. Prepare test conditions – nothing to set; we only read what the admin provided.

    // 3. Test code – attempt to parse the configured umask.
    try {
      FsPermission umask = FsPermission.getUMask(conf);
      // 4. If we reach here the value is syntactically valid.
      assertNotNull("UMask must not be null", umask);
    } catch (IllegalArgumentException iae) {
      fail("Invalid value for " + CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY +
           " : " + conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY));
    }
  }
}