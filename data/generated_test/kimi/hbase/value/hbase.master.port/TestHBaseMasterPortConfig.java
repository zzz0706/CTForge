package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.assertTrue;

import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseMasterPortConfig {

  private static final Pattern PORT_PATTERN = Pattern.compile("\\d{1,5}");

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMasterPortConfig.class);

  @Test
  public void testMasterPortValid() {
    Configuration conf = new Configuration();
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    String portStr = conf.get(HConstants.MASTER_PORT);
    if (portStr == null) {
      portStr = String.valueOf(HConstants.DEFAULT_MASTER_PORT);
    }
    // 2. Prepare the test conditions.
    // 3. Test code.
    assertTrue("hbase.master.port must be an integer between 0 and 65535",
        PORT_PATTERN.matcher(portStr).matches());
    int port = Integer.parseInt(portStr);
    assertTrue("hbase.master.port must be between 0 and 65535", port >= 0 && port <= 65535);
    // 4. Code after testing.
  }
}