package org.apache.hadoop.hbase.procedure;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, MediumTests.class})
public class TestProcedureManagerHostConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProcedureManagerHostConfigValidation.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUp() {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    conf = HBaseConfiguration.create();
  }

  @AfterClass
  public static void tearDown() {
    conf = null;
  }

  @Test
  public void testMasterProcedureClassesValid() {
    // 2. Prepare the test conditions.
    String[] classes = conf.getStrings(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);
    if (classes == null || classes.length == 0) {
      // Empty is allowed
      return;
    }

    // 3. Test code.
    for (String className : classes) {
      className = className.trim();
      if (className.isEmpty()) {
        fail("Empty class name found in " + ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);
      }

      ClassLoader cl = Thread.currentThread().getContextClassLoader();
      try {
        Class<?> implClass = cl.loadClass(className);
        if (!MasterProcedureManager.class.isAssignableFrom(implClass)) {
          fail("Class " + className + " does not implement MasterProcedureManager");
        }
      } catch (ClassNotFoundException e) {
        fail("Class " + className + " configured in " +
             ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY + " not found on classpath");
      }
    }
    // 4. Code after testing.
  }

  @Test
  public void testMasterProcedureClassesWhitespaceHandling() {
    // 2. Prepare the test conditions.
    String raw = conf.get(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);
    if (raw == null) {
      return;
    }

    // 3. Test code.
    String[] split = raw.split(",");
    for (String part : split) {
      String trimmed = part.trim();
      if (!trimmed.equals(part)) {
        assertTrue("Leading/trailing whitespace detected in class name: " + part,
                   trimmed.equals(part));
      }
    }
    // 4. Code after testing.
  }
}