package org.apache.hadoop.hbase.master.cleaner;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestHBaseMasterLogCleanerPluginsConfig {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestHBaseMasterLogCleanerPluginsConfig.class);

  /**
   * Verify that the value of "hbase.master.logcleaner.plugins"
   * 1. is a comma-separated list of fully-qualified class names,
   * 2. contains at least the two default cleaners,
   * 3. contains only loadable classes that implement BaseLogCleanerDelegate.
   */
  @Test
  public void testLogCleanerPluginsConfiguration() {
    Configuration conf = new Configuration();
    // Ensure we load the default HBase configuration
    conf.addResource("hbase-default.xml");
    conf.addResource("hbase-site.xml");

    // Fetch the plugins list, falling back to the default if not explicitly set
    String[] plugins = conf.getStrings(
        HConstants.HBASE_MASTER_LOGCLEANER_PLUGINS,
        TimeToLiveLogCleaner.class.getName() + ","
            + TimeToLiveProcedureWALCleaner.class.getName());

    if (plugins == null || plugins.length == 0) {
      fail("hbase.master.logcleaner.plugins must not be empty");
    }

    List<String> pluginList = Arrays.asList(plugins);
    assertTrue("Default TimeToLiveLogCleaner is missing",
        pluginList.contains(TimeToLiveLogCleaner.class.getName()));
    assertTrue("Default TimeToLiveProcedureWALCleaner is missing",
        pluginList.contains(TimeToLiveProcedureWALCleaner.class.getName()));

    // Ensure every listed class can be loaded and is assignable from BaseLogCleanerDelegate
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    for (String cls : pluginList) {
      try {
        Class<?> clazz = cl.loadClass(cls.trim());
        if (!BaseLogCleanerDelegate.class.isAssignableFrom(clazz)) {
          fail("Class " + cls + " does not implement BaseLogCleanerDelegate");
        }
      } catch (ClassNotFoundException e) {
        fail("Configured cleaner class " + cls + " not found: " + e.getMessage());
      }
    }
  }
}