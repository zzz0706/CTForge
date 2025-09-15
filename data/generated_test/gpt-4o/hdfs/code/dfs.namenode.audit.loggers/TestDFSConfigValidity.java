package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

public class TestDFSConfigValidity {

    /**
     * Tests the validity of the configuration item `dfs.namenode.audit.loggers`.
     * Ensures that the retrieved configuration satisfies the constraints and dependencies.
     */
    @Test
    public void testAuditLoggersConfigurationValidity() {
        // 1. Prepare the test configuration using the Hadoop Configuration API.
        Configuration conf = new Configuration();

        // Set the testing configuration value for dfs.namenode.audit.loggers
        // NOTE: The Hadoop documentation suggests the default value is "default".
        conf.set(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, "default");

        // 2. Test code for retrieving and validating the configuration values.
        Collection<String> auditLoggerClasses = conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);

        // Validate configuration
        try {
            // Case 1: If the configuration is empty or contains "default", ensure it's valid.
            if (auditLoggerClasses == null || auditLoggerClasses.isEmpty() || auditLoggerClasses.contains("default")) {
                assertTrue("The configuration defaults to the default audit logger.",
                        auditLoggerClasses == null || auditLoggerClasses.contains("default"));
            } else {
                // Case 2: Validate each class name provided in the configuration.
                for (String className : auditLoggerClasses) {
                    // Ensure that either the class is "default" or it exists in the classpath and implements AuditLogger.
                    if ("default".equals(className)) {
                        continue; // "default" case requires no additional validation.
                    }

                    // Dynamically load the class and check if it implements AuditLogger.
                    Class<?> clazz = Class.forName(className);
                    assertTrue("The class " + className + " must implement AuditLogger.",
                            AuditLogger.class.isAssignableFrom(clazz));

                    // Ensure the class can be instantiated and initialized successfully.
                    AuditLogger logger = (AuditLogger) clazz.getDeclaredConstructor().newInstance();
                    logger.initialize(conf);
                }
            }
        } catch (Exception e) {
            // Fail the test if any unexpected exception occurs during validation.
            fail("Unexpected exception during configuration validation: " + e.getMessage());
        }
    }
}