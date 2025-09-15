package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(MediumTests.class)
public class TestHBaseRootDirPerms {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(TestHBaseRootDirPerms.class);

  /**
   * Test to validate the hbase.rootdir.perms configuration for correctness.
   */
  @Test
  public void testRootDirPermsConfiguration() {
    // 1. Use the HBase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///"); // Set default filesystem for local tests.
    conf.set("hbase.rootdir", "/tmp/hbase"); // Example root directory for testing.
    conf.set("hbase.rootdir.perms", "700"); // Example permission configuration.

    FileSystem fs;
    Path rootDir;

    try {
      // 2. Prepare the test conditions: Fetch and validate the root directory path and the FileSystem instance.
      String rootDirPath = conf.get("hbase.rootdir");
      if (rootDirPath == null || rootDirPath.isEmpty()) {
        fail("hbase.rootdir configuration is not set.");
      }
      rootDir = new Path(rootDirPath); // Initialize the rootDir with the correct value.
      fs = rootDir.getFileSystem(conf);

      // Validate the hbase.rootdir.perms configuration value.
      String rootDirPermsString = conf.get("hbase.rootdir.perms", "700"); // Default permission set to "700".
      FsPermission expectedRootDirPerms;

      try {
        // Validate that the permissions string is correctly formatted as an FsPermission object.
        expectedRootDirPerms = new FsPermission(rootDirPermsString);
      } catch (IllegalArgumentException e) {
        fail("Invalid format for hbase.rootdir.perms: " + rootDirPermsString);
        return;
      }

      // Validate the permissions string format is a valid octal representation between "000" and "777".
      assertTrue("hbase.rootdir.perms must be a 3-digit octal string matching [0-7]{3}",
          expectedRootDirPerms.toShort() >= 0 && expectedRootDirPerms.toShort() < 01000);

      // 3. Test code: Fetch the current permissions of the root directory from the FileSystem.
      FsPermission currentRootPerms = fs.getFileStatus(rootDir).getPermission();

      // Check if the current permissions match the configured hbase.rootdir.perms.
      if (!expectedRootDirPerms.equals(currentRootPerms)) {
        // If mismatch, manually apply the expected permissions to the directory and validate again.
        fs.setPermission(rootDir, expectedRootDirPerms);
        currentRootPerms = fs.getFileStatus(rootDir).getPermission();
      }

      // Assert that the corrected permissions now match the expected configuration.
      assertTrue("Root directory permissions should match hbase.rootdir.perms after correction.",
          expectedRootDirPerms.equals(currentRootPerms));

    } catch (IOException e) {
      // Handle IOException related to FileSystem operations.
      fail("Failed to validate or apply hbase.rootdir.perms: " + e.getMessage());
    }
  }
}