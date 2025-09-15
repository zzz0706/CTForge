package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class TestFSNamesystemAuditLoggers {

    @Test
    // Test audit loggers with invalid logger class configuration
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testAuditLoggersWithInvalidLoggerClass() throws IOException {
        // 1. Prepare the test conditions: Initialize a Hadoop Configuration object and set 'dfs.namenode.audit.loggers' to an invalid class name.
        Configuration conf = new Configuration();

        // Set the configuration value for DFS_NAMENODE_AUDIT_LOGGERS_KEY using the API.
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "InvalidAuditLogger");

        FSImage fsImage = new FSImage(conf); // FSNamesystem requires FSImage as a constructor parameter.

        try {
            // 2. Test code: Create FSNamesystem instance and verify behavior by invoking getAuditLoggers.
            FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);

            // Invoke getAuditLoggers to trigger initAuditLoggers indirectly
            List<AuditLogger> auditLoggers = fsNamesystem.getAuditLoggers();

            // If no exception is thrown, the test should fail.
            assert false : "Expected RuntimeException was not thrown!";
        } catch (RuntimeException e) {
            // 3. Code after testing: Verify that a RuntimeException is thrown and contains a relevant message.
            assert e.getMessage().contains("InvalidAuditLogger") : "Unexpected error message: " + e.getMessage();
        } catch (Exception e) {
            // Fail the test if an unexpected exception type is thrown.
            assert false : "Unexpected exception type: " + e.getClass().getName();
        }
    }
}