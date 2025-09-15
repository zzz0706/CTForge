package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.security.UserGroupInformation;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ SecurityTests.class, SmallTests.class })
public class SuperusersTest {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(SuperusersTest.class);

  private Field superUsersField;
  private Field superGroupsField;

  @Before
  public void setUp() throws Exception {
    // Make the private fields accessible once
    superUsersField = Superusers.class.getDeclaredField("superUsers");
    superUsersField.setAccessible(true);
    superGroupsField = Superusers.class.getDeclaredField("superGroups");
    superGroupsField.setAccessible(true);
  }

  @After
  public void tearDown() throws Exception {
    // Reset the static state so every test starts from scratch
    superUsersField.set(null, null);
    superGroupsField.set(null, null);
  }

  @SuppressWarnings("unchecked")
  private Set<String> getSuperUsers() throws Exception {
    return (Set<String>) superUsersField.get(null);
  }

  @SuppressWarnings("unchecked")
  private Set<String> getSuperGroups() throws Exception {
    return (Set<String>) superGroupsField.get(null);
  }

  @Test
  public void testEmptyConfigAddsOnlyCurrentUser() throws Exception {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String expectedUser = "hbase";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(expectedUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    Set<String> superUsers = getSuperUsers();
    Set<String> superGroups = getSuperGroups();

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
    Set<String> superUsers = getSuperUsers();
    Set<String> superGroups = getSuperGroups();

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
    Set<String> superUsers = getSuperUsers();
    Set<String> superGroups = getSuperGroups();

    assertEquals("superUsers should contain only current user", 1, superUsers.size());
    assertTrue("superUsers should contain current user", superUsers.contains(currentUser));

    assertEquals("superGroups should contain 2 elements", 2, superGroups.size());
    assertTrue("superGroups should contain @hbase", superGroups.contains("@hbase"));
    assertTrue("superGroups should contain @root", superGroups.contains("@root"));
  }

  @Test
  public void testIsSuperUserPositive() throws Exception {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String currentUser = "current";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(currentUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    conf.setStrings("hbase.superuser", "alice,@admins");

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    User aliceUser = User.create(UserGroupInformation.createUserForTesting("alice", new String[] {}));
    assertTrue("alice should be recognized as super user", Superusers.isSuperUser(aliceUser));

    User adminUser = User.create(UserGroupInformation.createUserForTesting("bob", new String[] { "admins" }));
    assertTrue("bob in admins group should be recognized as super user", Superusers.isSuperUser(adminUser));
  }

  @Test
  public void testIsSuperUserNegative() throws Exception {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values.
    Configuration conf = new Configuration(false);

    // 2. Prepare the test conditions.
    String currentUser = "current";
    UserGroupInformation ugi = UserGroupInformation.createUserForTesting(currentUser, new String[] {});
    UserGroupInformation.setLoginUser(ugi);

    conf.setStrings("hbase.superuser", "alice,@admins");

    // 3. Test code.
    Superusers.initialize(conf);

    // 4. Code after testing.
    User plainUser = User.create(UserGroupInformation.createUserForTesting("plain", new String[] { "users" }));
    assertFalse("plain user should not be recognized as super user", Superusers.isSuperUser(plainUser));
  }
}