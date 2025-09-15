package org.apache.hadoop.hbase.coprocessor;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class TestHBaseCoprocessorMasterClassesConfig {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule classRule =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestHBaseCoprocessorMasterClassesConfig.class);

  private Configuration conf;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  /**
   * Verifies that the value of {@code hbase.coprocessor.master.classes}
   * is a comma-separated list of fully-qualified Java class names.
   * The test loads the actual configuration (no hard-coded values) and
   * validates every listed class:
   * <ol>
   *   <li>is non-empty</li>
   *   <li>is a syntactically valid class name</li>
   *   <li>is loadable from the current classpath</li>
   *   <li>implements {@link org.apache.hadoop.hbase.coprocessor.MasterObserver}</li>
   * </ol>
   */
  @Test
  public void testMasterCoprocessorClassesAreValid() {
    String[] classNames = conf.getStrings(CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY);

    // Empty or unset is legal – nothing to validate
    if (classNames == null || classNames.length == 0) {
      return;
    }

    List<String> invalid = Arrays.stream(classNames)
        .map(String::trim)
        .filter(cn -> !cn.isEmpty())
        .filter(cn -> !isValidMasterObserver(cn))
        .collect(Collectors.toList());

    if (!invalid.isEmpty()) {
      fail("Invalid master coprocessor classes in configuration: " + invalid);
    }

    assertTrue("All configured master coprocessor classes are valid", invalid.isEmpty());
  }

  private boolean isValidMasterObserver(String className) {
    if (!isJavaClassName(className)) {
      return false;
    }
    try {
      Class<?> clazz = Class.forName(className, false, Thread.currentThread().getContextClassLoader());
      return org.apache.hadoop.hbase.coprocessor.MasterObserver.class.isAssignableFrom(clazz);
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  private boolean isJavaClassName(String className) {
    if (className == null || className.isEmpty()) {
      return false;
    }
    for (String part : className.split("\\.")) {
      if (!part.matches("[A-Za-z_$][A-Za-z0-9_$]*")) {
        return false;
      }
    }
    return true;
  }
}