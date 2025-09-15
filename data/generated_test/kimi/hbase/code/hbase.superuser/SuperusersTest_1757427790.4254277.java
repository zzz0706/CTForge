package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, SmallTests.class })
public class SuperusersTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(SuperusersTest.class);

  @Test
  public void testEmptyConfigAddsOnlyCurrentUser() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String expectedUser = "hbase";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(expectedUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    Field superUsersField = Superusers.class.getDeclaredField("superUsers");
    superUsersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superUsers = (Set<String>) superUsersField.get(null);

    Field superGroupsField = Superusers.class.getDeclaredField("superGroups");
    superGroupsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superGroups = (Set<String>) superGroupsField.get(null);

    assertEquals("superUsers should contain exactly one element", 1, superUsers.size());
    assertTrue("superUsers should contain the current user", superUsers.contains(expectedUser));
    assertTrue("superGroups should be empty", superGroups.isEmpty());
  }

  @Test
  public void testConfigWithUsersAndGroups() throws Exception {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String currentUser = "current";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(currentUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    conf.setStrings("hbase.superuser", "alice,bob,@admins,@ops");

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    Field superUsersField = Superusers.class.getDeclaredField("superUsers");
    superUsersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superUsers = (Set<String>) superUsersField.get(null);

    Field superGroupsField = Superusers.class.getDeclaredField("superGroups");
    superGroupsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superGroups = (Set<String>) superGroupsField.get(null);

    assertEquals("superUsers should contain 3 elements", 3, superUsers.size());
    assertTrue("superUsers should contain current user", superUsers.contains(currentUser));
    assertTrue("superUsers should contain alice", superUsers.contains("alice"));
    assertTrue("superUsers should contain bob", superUsers.contains("bob"));

    assertEquals("superGroups should contain 2 elements", 2, superGroups.size());
    assertTrue("superGroups should contain @admins", superGroups.contains("@admins"));
    assertTrue("superGroups should contain @ops", superGroups.contains("@ops"));
  }

  @Test
  public void testConfigWithOnlyGroups() throws Exception {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String currentUser = "current";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(currentUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    conf.setStrings("hbase.superuser", "@hbase,@root");

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    Field superUsersField = Superusers.class.getDeclaredField("superUsers");
    superUsersField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superUsers = (Set<String>) superUsersField.get(null);

    Field superGroupsField = Superusers.class.getDeclaredField("superGroups");
    superGroupsField.setAccessible(true);
    @SuppressWarnings("unchecked")
    Set<String> superGroups = (Set<String>) superGroupsField.get(null);

    assertEquals("superUsers should contain only current user", 1, superUsers.size());
    assertTrue("superUsers should contain current user", superUsers.contains(currentUser));

    assertEquals("superGroups should contain 2 elements", 2, superGroups.size());
    assertTrue("superGroups should contain @hbase", superGroups.contains("@hbase"));
    assertTrue("superGroups should contain @root", superGroups.contains("@root"));
  }
}