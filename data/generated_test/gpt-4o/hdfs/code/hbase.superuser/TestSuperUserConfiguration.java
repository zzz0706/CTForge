package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * TestSuperUserConfiguration validates the configuration propagation and usage of `hbase.superuser`.
 * This test ensures correct initialization and usage when valid user names are provided.
 */
@Category({SecurityTests.class, SmallTests.class})
public class TestSuperUserConfiguration {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSuperUserConfiguration.class);

    @Test
    public void testSuperUserInitializationWithValidUsers() throws Exception {
        // 1. Use the HBase 2.2.2 API to configure test values dynamically.
        Configuration conf = new Configuration();
        conf.setStrings(Superusers.SUPERUSER_CONF_KEY, "user1", "user2");

        // 2. Prepare the test: call the initialization function.
        Superusers.initialize(conf);

        // 3. Retrieve the superUsers and validate them using the expected behavior of the API.
        Collection<String> superUsers = Superusers.getSuperUsers();
        String currentProcessUser = User.getCurrent().getShortName();

        // Verify that 'user1' and 'user2' are correctly added to superUsers.
        assertTrue("Expected 'user1' to be a superuser.", superUsers.contains("user1"));
        assertTrue("Expected 'user2' to be a superuser.", superUsers.contains("user2"));

        // Verify that the current process user is always a superuser.
        assertTrue("Expected current process user to be a superuser.", superUsers.contains(currentProcessUser));

        // 4. Clean up after testing if necessary (not applicable for static configurations in this scenario).
    }
}