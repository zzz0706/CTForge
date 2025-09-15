package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.apache.hadoop.hbase.HBaseClassTestRule;

@Category({SecurityTests.class, SmallTests.class})
public class SuperusersTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(SuperusersTest.class);

  private Configuration conf;

  @Before
  public void setUp() {
    conf = HBaseConfiguration.create();
  }

  @After
  public void tearDown() {
    // reset the static state via reflection
    try {
      java.lang.reflect.Field su = Superusers.class.getDeclaredField("superUsers");
      su.setAccessible(true);
      su.set(null, null);

      java.lang.reflect.Field sg = Superusers.class.getDeclaredField("superGroups");
      sg.setAccessible(true);
      sg.set(null, null);
    } catch (Exception e) {
      throw new RuntimeException("Failed to reset static fields", e);
    }
  }

  @Test
  public void testGroupPrincipalParsedCorrectly() throws IOException {
    // 1. Configuration as Input
    conf.set(Superusers.SUPERUSER_CONF_KEY, "@admins");

    // 2. Prepare the test conditions
    // (HBase 2.2.2 uses Hadoop’s UserGroupInformation; we cannot mock it easily,
    //  so we rely on the fact that the current user’s short name will be picked up.)

    // 3. Test code
    Superusers.initialize(conf);

    // 4. Assertions and Verification
    Collection<String> superUsers = Superusers.getSuperUsers();
    Collection<String> superGroups = Superusers.getSuperGroups();

    assertEquals("superUsers should contain only the current user", 1, superUsers.size());
    assertTrue("superUsers should contain current user short name",
               superUsers.contains(System.getProperty("user.name")));

    assertEquals("superGroups should contain only the group principal", 1, superGroups.size());
    assertTrue("superGroups should contain 'admins'", superGroups.contains("@admins"));
  }
}