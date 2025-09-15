package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestVersionFileWriteAttemptsValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestVersionFileWriteAttemptsValidation.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    // Load the actual configuration file(s) without setting any value in code
    conf = new Configuration();
    conf.addResource("hbase-site.xml");
    conf.addResource("hbase-default.xml");
  }

  @Test
  public void testVersionFileWriteAttemptsIsPositive() {
    int attempts = conf.getInt(HConstants.VERSION_FILE_WRITE_ATTEMPTS,
                               HConstants.DEFAULT_VERSION_FILE_WRITE_ATTEMPTS);
    assertTrue("hbase.server.versionfile.writeattempts must be a positive integer",
               attempts > 0);
  }
}