package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestTimeToLiveLogCleanerConfigValidation {

  private static final String TTL_CONF_KEY = "hbase.master.logcleaner.ttl";

  private Configuration conf;

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule HBASE_CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestTimeToLiveLogCleanerConfigValidation.class);

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  @Test
  public void testTtlIsPositiveLong() {
    String value = conf.get(TTL_CONF_KEY);
    if (value != null) {
      try {
        long ttl = Long.parseLong(value.trim());
        assertTrue("hbase.master.logcleaner.ttl must be a positive long: " + value, ttl > 0);
      } catch (NumberFormatException e) {
        assertFalse("hbase.master.logcleaner.ttl is not a valid long: " + value, true);
      }
    }
  }

  @Test
  public void testTtlDefaultIsValid() {
    long ttl = conf.getLong(TTL_CONF_KEY, 600000L);
    assertTrue("Default hbase.master.logcleaner.ttl must be positive", ttl > 0);
  }
}