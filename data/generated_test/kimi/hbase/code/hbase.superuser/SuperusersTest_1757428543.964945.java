package org.apache.hadoop.hbase.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

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
    // Create a fresh configuration for every test
    conf = HBaseConfiguration.create();
  }

  @After
  public void tearDown() throws Exception {
    // Superusers keeps state in static fields; we re-initialize in every test
    // so no explicit reset is required.
  }

  @Test
  public void testIsSuperUserReturnsTrueForConfiguredPrincipal() throws IOException {
    // 1. Use the hbase 2.2.2 API correctly to obtain configuration values
    conf.set(Superusers.SUPERUSER_CONF_KEY, "charlie,@devs");

    // 2. Prepare the test conditions
    User charlieUser = User.createUserForTesting(conf, "charlie", new String[]{"devs"});
    User daveUser   = User.createUserForTesting(conf, "dave",   new String[]{"users"});

    // 3. Test code
    Superusers.initialize(conf);
    assertTrue(Superusers.isSuperUser(charlieUser));
    assertEquals(false, Superusers.isSuperUser(daveUser));

    // 4. Code after testing handled implicitly by setUp/tearDown
  }
}