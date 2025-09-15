package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.*;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestAuditLoggerConfigValidation {

  private Configuration conf;

  @Before
  public void setUp() {
    // 1. Use HDFS 2.8.5 API to obtain configuration values from the runtime environment
    conf = new Configuration();
  }

  @After
  public void tearDown() {
    conf = null;
  }

  /**
   * Validates that the value(s) supplied for dfs.namenode.audit.loggers
   * are either the literal "default" or fully-qualified class names that
   * implement org.apache.hadoop.hdfs.server.namenode.AuditLogger.
   */
  @Test
  public void testAuditLoggersConfigValidity() {
    // 2. Prepare test conditions: read the configured value(s) without altering them
    Collection<String> alClasses =
        conf.getTrimmedStringCollection(DFSConfigKeys.DFS_NAMENODE_AUDIT_LOGGERS_KEY);

    // 3. Test code: verify each configured value
    for (String className : alClasses) {
      if (DFSConfigKeys.DFS_NAMENODE_DEFAULT_AUDIT_LOGGER_NAME.equals(className)) {
        // "default" is explicitly allowed
        continue;
      }

      // Attempt to load the class and ensure it implements AuditLogger
      try {
        Class<?> clazz = Class.forName(className);
        if (!AuditLogger.class.isAssignableFrom(clazz)) {
          fail("Configured audit logger class '" + className
              + "' does not implement " + AuditLogger.class.getName());
        }
      } catch (ClassNotFoundException e) {
        fail("Configured audit logger class '" + className + "' not found on classpath");
      }
    }

    // 4. Code after testing: nothing to clean up
  }
}