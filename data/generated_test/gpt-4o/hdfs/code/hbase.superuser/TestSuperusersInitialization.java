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
 * TestSuperusersInitialization ensures that the correct behavior occurs during initialization
 * and usage of configuration related to superusers in HBase 2.2.2.
 */
@Category({SecurityTests.class, SmallTests.class})
public class TestSuperusersInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSuperusersInitialization.class);

    @Test
    public void testSuperUserInitialization_withValidUsers() throws Exception {
        // 1. Use the HBase 2.2.2 API to configure the test conditions.
        Configuration conf = new Configuration();
        conf.setStrings(Superusers.SUPERUSER_CONF_KEY, "user1", "user2");

        // 2. Prepare the test conditions: Initialize the superuser list.
        Superusers.initialize(conf);

        // 3. Retrieve the list of superusers and validate them.
        Collection<String> superUsers = Superusers.getSuperUsers();
        String currentProcessUser = User.getCurrent().getShortName();

        // Verify that the configured users are added to superUsers.
        assertTrue("Expected 'user1' to be a superuser.", superUsers.contains("user1"));
        assertTrue("Expected 'user2' to be a superuser.", superUsers.contains("user2"));

        // Verify the current process user is always treated as a superuser.
        assertTrue("Expected current process user to be a superuser.", superUsers.contains(currentProcessUser));

        // 4. Ensure no residual state affecting other tests (static state cleanup not applicable here).
    }
}