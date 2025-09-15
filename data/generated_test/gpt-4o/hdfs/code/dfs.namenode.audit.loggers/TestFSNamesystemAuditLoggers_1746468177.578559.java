package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

public class TestFSNamesystemAuditLoggers {

    @Test
    // Test audit loggers with invalid logger class configuration
    // 1. Use the hdfs 2.8.5 API correctly to obtain configuration values instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test the behavior by invoking relevant FSNamesystem methods.
    // 4. Verify the result.
    public void testAuditLoggersWithInvalidLoggerClass() throws IOException { // Ensure IOException is declared to avoid compilation errors
        // 1. Prepare the test conditions: Initialize a Hadoop Configuration object and set 'dfs.namenode.audit.loggers' to an invalid class name.
        Configuration conf = new Configuration();
        FSImage fsImage = new FSImage(conf); // FSNamesystem requires FSImage as a constructor parameter.

        // Set the configuration value for dfs.namenode.audit.loggers to an invalid class name.
        conf.set("dfs.namenode.audit.loggers", "InvalidAuditLogger");

        try {
            // 2. Test code: Create FSNamesystem instance and verify behavior by invoking initAuditLoggers indirectly through its usage in getAuditLoggers.
            FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
            
            // Invoke public method getAuditLoggers to indirectly trigger initAuditLoggers
            List<AuditLogger> auditLoggers = fsNamesystem.getAuditLoggers(); 

            // If no exception is thrown, the test should fail.
            assert false : "Expected RuntimeException was not thrown!";
        } catch (RuntimeException e) {
            // 3. Code after testing: Verify that a RuntimeException is thrown with the expected message.
            assert e.getMessage().contains("InvalidAuditLogger") : "Unexpected error message: " + e.getMessage();
        } catch (Exception e) {
            // Fail the test if any other unexpected exception is thrown.
            assert false : "Unexpected exception type: " + e.getClass().getName();
        }
    }
}