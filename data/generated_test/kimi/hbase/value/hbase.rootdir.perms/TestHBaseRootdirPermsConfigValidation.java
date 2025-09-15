package org.apache.hadoop.hbase.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

@Category({MasterTests.class, SmallTests.class})
public class TestHBaseRootdirPermsConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseRootdirPermsConfigValidation.class);

  /**
   * Tests that the value of hbase.rootdir.perms is a valid octal permission string
   * between 000 and 777. The configuration is read from the Configuration object
   * without setting any value in the test code.
   */
  @Test
  public void testHBaseRootdirPermsValidOctalPermission() {
    Configuration conf = new Configuration();
    // Load the configuration from the classpath without overriding any value
    // so we read whatever the user has configured.
    String perms = conf.get("hbase.rootdir.perms", "700");

    // Ensure the value is non-null and non-empty
    assertNotNull("hbase.rootdir.perms must not be null", perms);
    assertFalse("hbase.rootdir.perms must not be empty", perms.trim().isEmpty());

    // Ensure the value is a 3-digit octal string
    assertTrue("hbase.rootdir.perms must be a 3-digit octal string",
        perms.matches("^[0-7]{3}$"));

    // Ensure the value can be parsed as an octal FsPermission
    FsPermission fsPerm = FsPermission.createImmutable(
        Short.parseShort(perms, 8));

    // Ensure the parsed permission is within the valid range (000–777)
    short octal = Short.parseShort(perms, 8);
    assertTrue("hbase.rootdir.perms octal value must be between 000 and 777",
        octal >= 0 && octal <= 0777);
  }
}