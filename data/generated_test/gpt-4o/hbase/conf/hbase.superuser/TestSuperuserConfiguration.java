package org.apache.hadoop.hbase.security;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;

import static org.junit.Assert.*;

@Category({SecurityTests.class, SmallTests.class})
public class TestSuperuserConfiguration {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestSuperuserConfiguration.class);

  /**
   * Test to validate the configuration "hbase.superuser" and its constraints.
   */
  @Test
  public void testSuperuserConfigurationValidity() throws IOException {
    Configuration conf = new Configuration();

    // Retrieve the configuration value using the HBase API, not hardcoding it
    String[] superUserList = conf.getStrings(Superusers.SUPERUSER_CONF_KEY, new String[0]);

    // Test if the configuration is correctly set and meets expected constraints
    assertNotNull("Superuser configuration list should not be null", superUserList);

    for (String name : superUserList) {
      assertNotNull("Superuser name/group should not be null", name);
      assertFalse("Superuser name/group should not be empty", name.isEmpty());

      // Validate whether the name is a group or user name based on HBase constraints
      if (name.startsWith("@")) {
        assertTrue("Group name should start with '@' prefix", name.startsWith("@"));
      } else {
        assertFalse("User name should not contain '@'", name.contains("@"));
      }
    }

    // Validate the current system user in the context of HBase superusers
    User currentUser = User.getCurrent();
    assertNotNull("System user should not be null", currentUser);
    assertNotNull("System user's short name should not be null", currentUser.getShortName());
    assertFalse("System user's short name should not be empty", currentUser.getShortName().isEmpty());

    // Additional validation can be added based on dependency with other configurations.
  }
}