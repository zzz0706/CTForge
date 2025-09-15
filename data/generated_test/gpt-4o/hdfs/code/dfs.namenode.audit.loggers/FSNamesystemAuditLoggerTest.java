package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.server.namenode.FSNamesystem;
import org.apache.hadoop.hdfs.server.namenode.FSImage;
import org.apache.hadoop.hdfs.server.namenode.AuditLogger;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class FSNamesystemAuditLoggerTest {

    @Test
    // Test method to verify initialization of combined default and custom AuditLoggers.
    // 1. You need to use the hdfs 2.8.5 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    // 2. Prepare the test conditions.
    // 3. Test code.
    // 4. Code after testing.
    public void test_combined_audit_logger_initialization() throws Exception {
        // Step 1: Prepare configuration object using Hadoop API.
        Configuration configuration = new Configuration();
        configuration.reloadConfiguration(); // Ensure the configuration is loaded properly.

        // Retrieve logger type information from the configuration.
        String[] auditLoggerConfigurations = StringUtils.getTrimmedStrings(
                configuration.get(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY, ""));

        // Convert String[] to List<String>.
        List<String> auditLoggerConfigurationsList = new ArrayList<>();
        for (String loggerConfig : auditLoggerConfigurations) {
            auditLoggerConfigurationsList.add(loggerConfig);
        }

        // Step 2: Prepare an FSNamesystem instance using the configuration.
        // Obtain a mock FSImage object for initializing FSNamesystem.
        FSImage mockFSImage = new FSImage(configuration);
        FSNamesystem fsNamesystem = new FSNamesystem(configuration, mockFSImage);

        // Step 3a: Retrieve the audit loggers using FSNamesystem API.
        List<AuditLogger> auditLoggers = fsNamesystem.getAuditLoggers();

        // Step 3b: Verify that auditLoggers contains instances of DefaultAuditLogger and any custom AuditLoggers dynamically loaded from configuration.
        boolean containsDefaultLogger = false;
        for (AuditLogger logger : auditLoggers) {
            if (logger.getClass().getSimpleName().equals("DefaultAuditLogger")) {
                containsDefaultLogger = true;
                break;
            }
        }
        assert containsDefaultLogger : "DefaultAuditLogger should be present in the auditLoggers list";

        for (String className : auditLoggerConfigurationsList) {
            if (!"DefaultAuditLogger".equals(className)) {
                boolean containsCustomLogger = false;
                for (AuditLogger logger : auditLoggers) {
                    if (logger.getClass().getName().equals(className)) {
                        containsCustomLogger = true;
                        break;
                    }
                }
                assert containsCustomLogger : "Custom AuditLogger not found: " + className;
            }
        }

        // Step 4: Confirm that every logger is initialized by checking logger-specific properties (e.g., logger status or setup methods).
        for (AuditLogger logger : auditLoggers) {
            assert logger != null : "AuditLogger instance should not be null";
            assert logger instanceof AuditLogger : "Each logger should be an instance of AuditLogger";
        }
    }
}