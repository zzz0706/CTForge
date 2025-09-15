package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test class to validate the configuration values related to
 * dfs.namenode.resource.check.interval and its dependencies in HDFS 2.8.5.
 */
public class TestDFSConfigValidation {

  /**
   * Test configuration validation for dfs.namenode.resource.check.interval.
   * Ensures the configuration value satisfies constraints and dependencies.
   */
  @Test
  public void testResourceCheckIntervalConfiguration() {
    // 1. Use the HDFS 2.8.5 API correctly to obtain configuration values.
    Configuration conf = new Configuration();

    // Obtain the configuration value for dfs.namenode.resource.check.interval.
    long resourceCheckInterval = conf.getLong(
        DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_KEY,
        DFSConfigKeys.DFS_NAMENODE_RESOURCE_CHECK_INTERVAL_DEFAULT);

    // Constraint check: Validate the interval value is positive and not negative.
    Assert.assertTrue("Resource check interval must be positive.",
        resourceCheckInterval > 0);

    // 2. Prepare the test conditions for additional configurations.
    // Obtain related configuration values using the HDFS API.

    // dfs.namenode.resource.du.reserved: The reserved space for disk usage, must be non-negative.
    long reservedSpace = conf.getLong(
        "dfs.namenode.resource.du.reserved", // Corrected key from source code.
        0); // Default value set to 0 if key is not found.

    // dfs.namenode.resource.checked.volumes.minimum: Minimum number of volumes to check, must be non-negative.
    int minimumVolumes = conf.getInt(
        "dfs.namenode.resource.checked.volumes.minimum", // Corrected key from the source code.
        1); // Default value set to 1 for minimum volumes to check if key is not found.

    // 3. Test code for validations.

    // Validate that reserved space is non-negative.
    Assert.assertTrue("Reserved space must be non-negative.",
        reservedSpace >= 0);

    // Validate that minimum volumes is greater than or equal to zero.
    Assert.assertTrue("Minimum volumes must be non-negative.",
        minimumVolumes >= 0);

    // 4. Code after the testing phase.
    // No further constraints to validate for these configurations within the scope of this test.
  }
}