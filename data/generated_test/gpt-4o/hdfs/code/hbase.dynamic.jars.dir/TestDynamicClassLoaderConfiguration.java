package org.apache.hadoop.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestDynamicClassLoaderConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestDynamicClassLoaderConfiguration.class);

  /**
   * Test for validating the configuration of `hbase.dynamic.jars.dir`.
   * It ensures that the configuration value satisfies the constraints and dependencies.
   */
  @Test
  public void testDynamicJarsDirConfiguration() {
    // 1. Prepare the test setup.
    Configuration conf = new Configuration();
    conf.set("hbase.dynamic.jars.dir", "/tmp/hbase-dynamic-jars-dir"); // Set a test value for the configuration.
    conf.set("hbase.rootdir", "/tmp/hbase-rootdir"); // Set a test value for the rootdir configuration.

    // Step 1: Retrieve the configuration value using the HBase 2.2.2 API correctly.
    String dynamicJarsDir = conf.get("hbase.dynamic.jars.dir");

    // Step 2: Verify the configuration value is not null or empty.
    assertNotNull("Configuration 'hbase.dynamic.jars.dir' should not be null.", dynamicJarsDir);
    assertFalse("Configuration 'hbase.dynamic.jars.dir' should not be empty.", dynamicJarsDir.trim().isEmpty());

    // Step 3: Check if the configuration value defines a valid local directory.
    File localDir = new File(dynamicJarsDir);
    assertTrue("Configuration 'hbase.dynamic.jars.dir' should define a directory, but it doesn't.", 
               // Ensuring the directory exists or creating it for testing.
               localDir.exists() || localDir.mkdirs());
    assertTrue("Configuration 'hbase.dynamic.jars.dir' should define a directory, but it's a file.", localDir.isDirectory());

    // Step 4: Ensure the directory can be accessed properly.
    assertTrue("Configuration 'hbase.dynamic.jars.dir' is defined, but the directory is not accessible.",
               localDir.canRead() && localDir.canWrite());

    // Step 5: Test remote directory initialization if applicable.
    try {
      Path remotePath = new Path(dynamicJarsDir);
      FileSystem remoteDirFs = remotePath.getFileSystem(conf);

      // Validate the remote directory (if not the same as the local directory).
      if (!dynamicJarsDir.equals(conf.get("hbase.rootdir") + "/lib")) {
        assertNotNull("Filesystem for the remote directory defined in 'hbase.dynamic.jars.dir' should not be null.", remoteDirFs);
        assertTrue("Remote directory filesystem defined in 'hbase.dynamic.jars.dir' should be valid.",
                   remoteDirFs.exists(remotePath));
      }
    } catch (IOException ioe) {
      fail("Configuration 'hbase.dynamic.jars.dir' points to a remote directory, but it failed to initialize. Check filesystem permissions.");
    }

    // Step 6: Validate dependencies and constraints (if any defined in the source code).
    String rootDir = conf.get("hbase.rootdir");
    if (dynamicJarsDir.equals(rootDir + "/lib")) {
      assertNotNull("Configuration 'hbase.rootdir' should not be null if 'hbase.dynamic.jars.dir' is derived from it.", rootDir);
      assertFalse("Configuration 'hbase.rootdir' should not be empty if 'hbase.dynamic.jars.dir' is derived from it.", rootDir.trim().isEmpty());
    }
  }
}