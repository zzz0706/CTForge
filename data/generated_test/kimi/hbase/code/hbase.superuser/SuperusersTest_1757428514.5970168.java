package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;

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

  @Before
  public void setUp() throws Exception {
    // Superusers has no public reset(); we rely on initialize(Configuration) to overwrite state
  }

  @After
  public void tearDown() throws Exception {
    // No public reset available; each test will re-initialize anyway
  }

  @Test
  public void testIsSuperUserReturnsTrueForConfiguredPrincipal() throws Exception {
    // 1. You need to use the hbase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.superuser", "charlie,@devs");

    // 2. Prepare the test conditions.
    // HBase 2.2.2 uses org.apache.hadoop.hbase.security.User instead of org.apache.hadoop.security.User
    User charlieUser = User.createUserForTesting(conf, "charlie", new String[]{"devs"});
    User daveUser = User.createUserForTesting(conf, "dave", new String[]{"users"});

    // 3. Test code.
    Superusers.initialize(conf);
    assertEquals(true, Superusers.isSuperUser(charlieUser));
    assertEquals(false, Superusers.isSuperUser(daveUser));

    // 4. Code after testing.
    // Reset is handled by tearDown()
  }
}