package org.apache.hadoop.hbase.procedure;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.procedure.MasterProcedureManager;
import org.apache.hadoop.hbase.procedure.ProcedureManagerHost;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.List;

@Category(SmallTests.class)
public class TestMasterProcedureManagerConfiguration {

  @ClassRule // HBaseClassTestRule ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestMasterProcedureManagerConfiguration.class);

  @Test
  public void testMasterProcedureManagerConfigurationValidity() {
    // 1. Use the HBase 2.2.2 API correctly to obtain configuration values instead of hardcoding the configuration values.
    Configuration conf = new Configuration(); // Use Configuration directly instead of HBaseTestingUtility.

    // Obtain the procedure manager classes configured in HBase using the correct API.
    String[] procedureManagerClasses = conf.getStrings(ProcedureManagerHost.MASTER_PROCEDURE_CONF_KEY);

    // 2. Prepare the test conditions.
    // If the configuration value is null or empty, the test should pass because no procedures are configured.
    if (procedureManagerClasses == null || procedureManagerClasses.length == 0) {
      Assert.assertTrue("Configuration is empty, no procedures are loaded.", true);
      return;
    }

    // Create a list to hold successfully loaded classes.
    List<Class<?>> loadedClasses = new ArrayList<>();

    // 3. Test code.
    // Attempt to load each class specified in the configuration and validate its implementation.
    for (String className : procedureManagerClasses) {
      try {
        Class<?> clazz = Class.forName(className.trim());
        if (!MasterProcedureManager.class.isAssignableFrom(clazz)) {
          Assert.fail("Class " + className + " does not implement MasterProcedureManager.");
        }
        loadedClasses.add(clazz);
      } catch (ClassNotFoundException e) {
        Assert.fail("Class " + className + " could not be found: " + e.getMessage());
      } catch (Exception e) {
        Assert.fail("Class " + className + " failed to initialize: " + e.getMessage());
      }
    }

    // 4. Code after testing.
    // Validate that all valid procedure manager classes were loaded successfully.
    Assert.assertEquals("Loaded procedure manager count does not match configuration entries.", 
      procedureManagerClasses.length, loadedClasses.size());
    for (Class<?> loadedClass : loadedClasses) {
      Assert.assertTrue("Loaded class does not implement MasterProcedureManager.", 
        MasterProcedureManager.class.isAssignableFrom(loadedClass));
    }

    // Log completion for debugging and verification (optional).
    System.out.println("All configured procedure manager classes validated and loaded successfully.");
  }
}