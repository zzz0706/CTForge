package org.apache.hadoop.hbase.replication;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import static org.junit.Assert.*;

/**
 * Unit test to verify the configuration for replication codec.
 * This test ensures the HBase replication codec value is valid
 * and compatible with supported codecs.
 */
@Category({org.apache.hadoop.hbase.testclassification.SmallTests.class})
public class TestReplicationCodecConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestReplicationCodecConfig.class);

  /**
   * Test case to verify the validity of the `hbase.replication.rpc.codec` configuration.
   */
  @Test
  public void testReplicationCodecConfiguration() {
    // Step 1: Create an HBase configuration object to read configurations.
    Configuration conf = HBaseConfiguration.create();

    // Step 2: Retrieve the value of `hbase.replication.rpc.codec` from the configuration.
    String replicationCodec = conf.get(HConstants.REPLICATION_CODEC_CONF_KEY);

    // Step 3: Prepare the test conditions and verify the validity of the configuration.
    assertNotNull("Replication codec should not be null", replicationCodec);
    assertFalse("Replication codec should not be empty", replicationCodec.isEmpty());

    // Step 4: Validate that the codec is among supported values.
    String[] validValues = {
        "org.apache.hadoop.hbase.codec.KeyValueCodecWithTags",
        "org.apache.hadoop.hbase.codec.KeyValueCodec"
    };
    boolean isValid = false;
    for (String validValue : validValues) {
      if (replicationCodec.equals(validValue)) {
        isValid = true;
        break;
      }
    }
    // Ensure the codec value matches one of the supported codecs.
    assertTrue(
        "Replication codec '" + replicationCodec + "' is not one of the valid values.",
        isValid
    );
  }
}