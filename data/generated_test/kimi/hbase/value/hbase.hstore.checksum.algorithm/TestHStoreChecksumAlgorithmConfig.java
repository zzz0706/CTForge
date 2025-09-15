package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category({RegionServerTests.class, SmallTests.class})
public class TestHStoreChecksumAlgorithmConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHStoreChecksumAlgorithmConfig.class);

  @Test
  public void testChecksumAlgorithmValidValues() {
    Configuration conf = new Configuration();
    // Do NOT set the value; read from the actual configuration files.

    String value = conf.get(HConstants.CHECKSUM_TYPE_NAME);
    if (value != null) {
      // Validate against allowed values: NULL, CRC32, CRC32C
      boolean valid = false;
      for (ChecksumType type : ChecksumType.values()) {
        if (type.getName().equalsIgnoreCase(value)) {
          valid = true;
          break;
        }
      }
      if (!valid) {
        fail("Invalid checksum algorithm '" + value + "'. Allowed values are: NULL, CRC32, CRC32C");
      }
    } else {
      // If not set, default is CRC32C, which is valid
      assertTrue("Default checksum type must be valid", ChecksumType.getDefaultChecksumType() != null);
    }
  }
}