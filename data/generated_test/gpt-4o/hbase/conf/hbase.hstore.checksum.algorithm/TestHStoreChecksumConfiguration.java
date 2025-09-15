package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.ChecksumType;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test class to validate the checksum algorithm configuration in HBase.
 */
@Category(SmallTests.class)
public class TestHStoreChecksumConfiguration {

  /**
   * HBaseClassTestRule definition for TestHStoreChecksumConfiguration class.
   */
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE = 
      HBaseClassTestRule.forClass(TestHStoreChecksumConfiguration.class);

  /**
   * Validates the configuration value for `hbase.hstore.checksum.algorithm` and ensures
   * it conforms to defined constraints in HBase 2.2.2.
   */
  @Test
  public void testChecksumAlgorithmConfiguration() {
    // 1. Use HBase 2.2.2 APIs appropriately to initialize the configuration.
    Configuration configuration = new Configuration();

    // Retrieve the configuration value for checksum algorithm.
    String checksumAlgorithm = configuration.get(HConstants.CHECKSUM_TYPE_NAME);

    // 2. Prepare valid test conditions with expected checksum algorithm values.
    String[] validAlgorithms = {"NULL", "CRC32", "CRC32C"};
    String defaultAlgorithm = ChecksumType.getDefaultChecksumType().getName(); // Default value as per HBase 2.2.2.

    // Use the default value if the retrieved configuration is null or empty.
    if (checksumAlgorithm == null || checksumAlgorithm.isEmpty()) {
      checksumAlgorithm = defaultAlgorithm;
    }

    // 3. Test the validity of the configuration value using assertions.
    boolean isValid = false;
    for (String validAlgorithm : validAlgorithms) {
      if (validAlgorithm.equals(checksumAlgorithm)) {
        isValid = true;
        break;
      }
    }

    // Assert the validity of the checksum algorithm configuration.
    try {
      assertTrue("Invalid configuration value for hbase.hstore.checksum.algorithm: " + checksumAlgorithm, isValid);
    } catch (Exception ex) {
      fail("Exception occurred while validating hbase.hstore.checksum.algorithm: " + ex.getMessage());
    }

    // 4. Code after testing, if necessary, for cleanup or further verification.
  }
}