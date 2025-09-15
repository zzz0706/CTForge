package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHStoreChecksumConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHStoreChecksumConfig.class);

  @Test
  public void testDefaultChecksumTypeIsCRC32C() {
    // 1. Create a fresh Configuration instance without any explicit overrides
    Configuration conf = new Configuration();

    // 2. Compute the expected value dynamically from the same Configuration
    String checksumName = conf.get(HConstants.CHECKSUM_TYPE_NAME, "CRC32C");
    ChecksumType expectedType = ChecksumType.nameToType(checksumName);

    // 3. Invoke the method under test
    ChecksumType actualType = HStore.getChecksumType(conf);

    // 4. Assert the returned value matches the expected default
    assertEquals("Default checksum algorithm should be CRC32C",
                 expectedType, actualType);
  }
}