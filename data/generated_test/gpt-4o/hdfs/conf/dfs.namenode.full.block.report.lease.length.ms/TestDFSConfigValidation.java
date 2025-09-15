package org.apache.hadoop.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test to validate the configuration constraints and dependencies
 * for dfs.namenode.full.block.report.lease.length.ms.
 */
public class TestDFSConfigValidation {

    @Test
    public void testFullBlockReportLeaseLengthValidation() {
        // Prepare the test conditions: Create a configuration instance to read settings
        Configuration conf = new Configuration();

        // Retrieve the value of dfs.namenode.full.block.report.lease.length.ms
        long leaseLengthMs = conf.getLong(
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS,
                DFSConfigKeys.DFS_NAMENODE_FULL_BLOCK_REPORT_LEASE_LENGTH_MS_DEFAULT);

        // Test code: Validate the lease length value
        // Constraint: leaseLengthMs must be greater than or equal to 1 ms
        assertTrue(
            "dfs.namenode.full.block.report.lease.length.ms must be greater than or equal to 1 ms",
            leaseLengthMs >= 1
        );

        // Code after testing: Log or signify test completion
        // No specific after-testing code here, as assert checks handle test validation.
    }
}