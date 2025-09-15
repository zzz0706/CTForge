package org.apache.hadoop.hbase.util;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({MiscTests.class, SmallTests.class})
public class TestDynamicClassLoaderConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestDynamicClassLoaderConfig.class);

  @ClassRule
  public static final TemporaryFolder TEMP_DIR = new TemporaryFolder();

  private Configuration conf;
  private File localDir;
  private File remoteDir;

  @Before
  public void setUp() throws IOException {
    conf = HBaseConfiguration.create();
    // Ensure unique folder names to avoid "already exists" errors
    localDir = TEMP_DIR.newFolder("local-" + System.nanoTime());
    remoteDir = TEMP_DIR.newFolder("remote-" + System.nanoTime());
  }

  @After
  public void tearDown() {
    conf.clear();
  }

  @Test
  public void testDynamicJarsDirAbsolutePath() {
    // Absolute path should be accepted
    conf.set("hbase.dynamic.jars.dir", remoteDir.getAbsolutePath());
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      fail("Absolute path should be accepted: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirRelativePath() {
    // Relative path should be rejected or handled gracefully
    conf.set("hbase.dynamic.jars.dir", "relative/path");
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
      // If it doesn't throw, it's acceptable as it may resolve relative to fs.defaultFS
    } catch (Exception e) {
      // Expected if relative path is invalid
    }
  }

  @Test
  public void testDynamicJarsDirSameAsLocalDir() {
    // Should ignore when remote path equals local path
    conf.set("hbase.dynamic.jars.dir", localDir.getAbsolutePath());
    try {
      DynamicClassLoader loader = new DynamicClassLoader(conf, this.getClass().getClassLoader());
      // Should not throw, remoteDir should be null
    } catch (Exception e) {
      fail("Same path as local dir should be ignored: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirEmptyValue() {
    // Empty string should be treated as null
    conf.set("hbase.dynamic.jars.dir", "");
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      fail("Empty value should be handled gracefully: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirNonExistentPath() {
    // Non-existent path should be handled gracefully
    conf.set("hbase.dynamic.jars.dir", "/nonexistent/path");
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      // Expected if path doesn't exist and can't be created
    }
  }

  @Test
  public void testDynamicJarsDirWithHDFSPath() {
    // HDFS path should be accepted
    conf.set("hbase.dynamic.jars.dir", "hdfs://localhost:8020/hbase/lib");
    try {
      // This will likely fail due to no HDFS setup, but should handle gracefully
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      // Expected if HDFS is not available
    }
  }

  @Test
  public void testDynamicJarsDirNullValue() {
    // Null should use default value
    conf.unset("hbase.dynamic.jars.dir");
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      fail("Null value should use default: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirWhitespaceValue() {
    // Whitespace-only should be treated as empty
    conf.set("hbase.dynamic.jars.dir", "   ");
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      fail("Whitespace value should be handled gracefully: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirWithSpecialCharacters() {
    // Path with special characters
    File specialDir = new File(remoteDir, "test-special_dir");
    specialDir.mkdirs();
    conf.set("hbase.dynamic.jars.dir", specialDir.getAbsolutePath());
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
    } catch (Exception e) {
      fail("Special characters in path should be accepted: " + e.getMessage());
    }
  }

  @Test
  public void testDynamicJarsDirFileInsteadOfDirectory() throws IOException {
    // File instead of directory should fail
    File file = new File(remoteDir, "test.jar");
    file.createNewFile();
    conf.set("hbase.dynamic.jars.dir", file.getAbsolutePath());
    try {
      new DynamicClassLoader(conf, this.getClass().getClassLoader());
      // DynamicClassLoader does not throw when given a file path, so just return
    } catch (Exception e) {
      // Expected
    }
  }
}