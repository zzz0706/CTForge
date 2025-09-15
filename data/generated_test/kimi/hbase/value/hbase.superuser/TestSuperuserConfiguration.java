package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SecurityTests.class, SmallTests.class})
public class TestSuperuserConfiguration {

  @ClassRule
  public static final org.apache.hadoop.hbase.HBaseClassTestRule CLASS_RULE =
      org.apache.hadoop.hbase.HBaseClassTestRule.forClass(TestSuperuserConfiguration.class);

  private static Configuration conf;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // 1. Obtain configuration values from the real configuration files instead of hard-coding
    conf = HBaseConfiguration.create();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    // 4. Cleanup after test
    conf.clear();
  }

  @Test
  public void testSuperuserConfigurationValueIsValid() throws IOException {
    // 2. Prepare test conditions
    String superuserList = conf.get(Superusers.SUPERUSER_CONF_KEY, "");

    // 3. Test code: verify the value satisfies constraints
    // Constraint: comma-separated list of users/groups, no embedded blanks, optional '@' prefix for groups
    if (!superuserList.trim().isEmpty()) {
      String[] entries = superuserList.split(",");
      for (String entry : entries) {
        assertNotNull("Superuser entry must not be null", entry);
        String trimmed = entry.trim();
        assertTrue("Superuser entry must not be blank", !trimmed.isEmpty());
        assertTrue("Superuser entry must not contain whitespace", trimmed.equals(entry));
        // Groups start with '@'; otherwise treated as user
        // No further validation on actual user/group existence here, only format
      }
    }
  }
}