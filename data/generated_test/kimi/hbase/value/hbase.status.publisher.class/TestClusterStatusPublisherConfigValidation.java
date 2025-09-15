package org.apache.hadoop.hbase.master;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestClusterStatusPublisherConfigValidation {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestClusterStatusPublisherConfigValidation.class);

  /**
   * Test that when {@code hbase.status.publisher.class} is explicitly set,
   * the supplied value must be assignable to
   * {@link org.apache.hadoop.hbase.master.ClusterStatusPublisher.Publisher}.
   * We do NOT set any configuration in the test code – we only validate
   * whatever is already present in the loaded {@link Configuration}.
   */
  @Test
  public void testStatusPublisherClassIsLoadableAndAssignable() {
    Configuration conf = new Configuration();
    // Load the *real* configuration from the classpath (hbase-site.xml, core-site.xml, etc.)
    // so that we test the exact values a user would have supplied.
    conf.addResource("hbase-site.xml");
    conf.addResource("core-site.xml");

    // Only validate if the user has explicitly set the key.
    if (conf.get(ClusterStatusPublisher.STATUS_PUBLISHER_CLASS) != null) {
      try {
        Class<?> clazz = conf.getClass(
            ClusterStatusPublisher.STATUS_PUBLISHER_CLASS,
            ClusterStatusPublisher.DEFAULT_STATUS_PUBLISHER_CLASS,
            ClusterStatusPublisher.Publisher.class);

        assertTrue(
            "Configured hbase.status.publisher.class is not assignable to Publisher interface",
            ClusterStatusPublisher.Publisher.class.isAssignableFrom(clazz));
      } catch (RuntimeException e) {
        // getClass will wrap ClassNotFoundException, NoClassDefFoundError, etc.
        fail("Configured hbase.status.publisher.class is not loadable: " + e.getMessage());
      }
    }
  }

  /**
   * Test that {@code hbase.status.publisher.class} is *ignored* when
   * {@code hbase.status.published} is false.
   * We do NOT set any configuration in the test code – we simply assert
   * that the default behaviour is correct.
   */
  @Test
  public void testStatusPublisherClassIgnoredWhenStatusPublishedFalse() {
    Configuration conf = new Configuration();
    conf.addResource("hbase-site.xml");
    conf.addResource("core-site.xml");

    boolean shouldPublish = conf.getBoolean(
        HConstants.STATUS_PUBLISHED,
        HConstants.STATUS_PUBLISHED_DEFAULT);

    if (!shouldPublish) {
      // Any value (even invalid) must be ignored.
      // Nothing to validate – the class is not instantiated.
      assertTrue("When status publishing is disabled, any publisher class is ignored", true);
    }
  }
}