package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestHBaseRootDirConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseRootDirConfig.class);

  @Test
  public void testHBaseRootDirIsFullyQualified() {
    Configuration conf = new Configuration();
    // Do NOT set hbase.rootdir here; read from any loaded conf file
    String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir != null) {
      assertTrue("hbase.rootdir must be fully-qualified (start with scheme://)",
          rootDir.contains("://"));
    }
  }

  @Test
  public void testHBaseRootDirIsNotEmpty() {
    Configuration conf = new Configuration();
    String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir != null) {
      assertFalse("hbase.rootdir must not be empty", rootDir.trim().isEmpty());
    }
  }
}