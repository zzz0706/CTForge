package org.apache.hadoop.hbase.snapshot;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.snapshot.SnapshotDescriptionUtils;
import org.apache.hadoop.hbase.shaded.protobuf.generated.SnapshotProtos.SnapshotDescription.Type;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertTrue;

@Category(SmallTests.class)
public class TestSnapshotDescriptionUtils {

    @ClassRule // Provides HBaseClassTestRule for proper test execution context
    public static final HBaseClassTestRule CLASS_RULE = HBaseClassTestRule.forClass(TestSnapshotDescriptionUtils.class);

    @Test
    public void testGetMaxMasterTimeout_ValidConfiguration() {
        // 1. Prepare the test conditions: Initialize a valid Configuration object.
        Configuration conf = new Configuration();

        // 2. Obtain configuration values using HBase APIs, no hardcoding of values.
        long defaultTimeout = SnapshotDescriptionUtils.DEFAULT_MAX_WAIT_TIME;
        long masterTimeout = conf.getLong(SnapshotDescriptionUtils.MASTER_SNAPSHOT_TIMEOUT_MILLIS, defaultTimeout);
        long snapshotTimeout = conf.getLong(SnapshotDescriptionUtils.SNAPSHOT_TIMEOUT_MILLIS_KEY, defaultTimeout);

        // 3. Validate the logic by calling the method under test.
        // Using the correct Type enum from the protobuf generated class.
        Type snapshotType = Type.DISABLED;

        long calculatedTimeout = SnapshotDescriptionUtils.getMaxMasterTimeout(conf, snapshotType, defaultTimeout);

        // 4. Assertions to check if the timeout is the maximum of the expected values.
        assertTrue("Calculated timeout should match the expected maximum value",
            calculatedTimeout == Math.max(masterTimeout, Math.max(snapshotTimeout, defaultTimeout)));
    }
}