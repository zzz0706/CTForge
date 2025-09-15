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

import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * TestSuperUserInitialization validates that the configuration `hbase.superuser`
 * is correctly parsed, and its functionality remains effective under given workloads.
 */
@Category({SecurityTests.class, SmallTests.class})
public class TestSuperUserInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSuperUserInitialization.class);

    @Test
    public void testSuperUserInitializationWithValidUsers() throws Exception {
        // Step 1: Prepare the configuration for testing.
        Configuration configuration = new Configuration();
        configuration.setStrings(Superusers.SUPERUSER_CONF_KEY, "user1", "user2");
        
        // Step 2: Call the initialization code.
        Superusers.initialize(configuration);
        
        // Step 3: Access the internal static collection for validation.
        Collection<String> superUsers = Superusers.getSuperUsers();
        String currentProcessUser = User.getCurrent().getShortName();

        // Step 4: Validate the expected superusers.
        assertTrue("Expected 'user1' to be a superuser.", superUsers.contains("user1"));
        assertTrue("Expected 'user2' to be a superuser.", superUsers.contains("user2"));
        assertTrue("Expected current process user to be a superuser.", superUsers.contains(currentProcessUser));
    }
}