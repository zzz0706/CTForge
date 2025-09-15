package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.ClassRule;

import static org.junit.Assert.*;

@Category(SmallTests.class)
public class TestMasterSnapshotTimeoutMillisConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterSnapshotTimeoutMillisConfiguration.class);

  @Test
  public void testMasterSnapshotTimeoutMillisConfiguration() {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration(); // Correct way to create Configuration instance in HBase.

    // Fetch configuration keys and defaults using the utility class correctly.
    final String masterSnapshotTimeoutKey = SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_KEY; 
    final long masterSnapshotTimeoutDefault = SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_DEFAULT;

    // 2. Prepare the test conditions.
    long snapshotTimeoutMillis = conf.getLong(masterSnapshotTimeoutKey, masterSnapshotTimeoutDefault);

    // 3. Test code.
    // Validate configuration key value.
    assertTrue(
        "The value of configuration '" + masterSnapshotTimeoutKey + "' must be a positive number.",
        snapshotTimeoutMillis > 0
    );

    // Validate calculated values using the official utility method.
    long calculatedTimeout = SnapshotDescriptionUtils.getMaxMasterTimeout(
        conf, SnapshotDescription.Type.FLUSH, masterSnapshotTimeoutDefault
    );

    // Ensure calculated timeout matches the expected maximum timeout.
    assertEquals(
        "Configuration propagation failed: calculated timeout should match the maximum configuration value.",
        snapshotTimeoutMillis,
        calculatedTimeout
    );

    // 4. Code after testing.
    System.out.println("Test passed successfully for configuration key: '" + masterSnapshotTimeoutKey + "'");
  }
}