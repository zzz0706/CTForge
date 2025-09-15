package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;

public class TestFSNamesystemAuditLoggers {

    @Test
    // test code
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void testAuditLoggersWithInvalidLoggerClass() throws IOException { // Ensure IOException is declared to avoid compilation errors
        // Prepare the test conditions: Initialize a Hadoop Configuration object and set 'dfs.namenode.audit.loggers' to an invalid class name
        Configuration conf = new Configuration();
        FSImage fsImage = new FSImage(conf); // FSNamesystem requires FSImage as a constructor parameter

        // Set configuration value for dfs.namenode.audit.loggers
        conf.set("dfs.namenode.audit.loggers", "InvalidAuditLogger");

        try {
            // Test code: Create FSNamesystem instance with the required parameters and invoke initAuditLoggers
            FSNamesystem fsNamesystem = new FSNamesystem(conf, fsImage);
            // Use reflection to access the private initAuditLoggers method
            java.lang.reflect.Method method = FSNamesystem.class.getDeclaredMethod("initAuditLoggers", Configuration.class);
            method.setAccessible(true); // Make the method accessible
            Collection<AuditLogger> auditLoggers = (Collection<AuditLogger>) method.invoke(fsNamesystem, conf);

            // If no exception is thrown, the test should fail
            assert false : "Expected RuntimeException was not thrown!";
        } catch (RuntimeException e) {
            // Code after testing: Verify that a RuntimeException is thrown with the expected message
            assert e.getMessage().contains("InvalidAuditLogger") : "Unexpected error message: " + e.getMessage();
        } catch (Exception e) {
            // Fail the test if any other unexpected exception is thrown
            assert false : "Unexpected exception type: " + e.getClass().getName();
        }
    }
}