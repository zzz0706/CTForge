package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

@Category({SecurityTests.class, SmallTests.class})
public class TestSuperusersInitialization {

    @ClassRule // Correct usage of HBaseClassTestRule
    public static final HBaseClassTestRule CLASS_RULE =
            HBaseClassTestRule.forClass(TestSuperusersInitialization.class);

    private Configuration conf;

    @Before
    public void setUp() {
        // Prepare the Configuration instance for the test
        conf = new Configuration();
    }

    @Test
    // Test to verify superuser initialization with an empty configuration
    public void testSuperUserInitialization_withEmptyConfiguration() throws Exception {
        // Prepare test conditions
        // Obtain the current user's short name using HBase's User class
        User currentUser = User.getCurrent();
        String expectedCurrentUser = currentUser != null ? currentUser.getShortName() : null;

        // Ensure currentUser is not null for the test to proceed correctly
        assertTrue("Current user cannot be null", expectedCurrentUser != null);

        // Test code
        // Initialize 'Superusers' with the empty configuration
        Superusers.initialize(conf);

        // Retrieve the contents of the superUsers and superGroups from Superusers
        Collection<String> superUsers = Superusers.getSuperUsers();
        Collection<String> superGroups = Superusers.getSuperGroups();

        // Assert that superUsers contain only the current process user
        assertTrue("Superusers should contain the current process user only",
                superUsers.contains(expectedCurrentUser));

        // Assert that superGroups are empty since no 'hbase.superuser' setting was configured
        assertEquals("SuperGroups should be empty for an empty 'hbase.superuser' configuration.",
                0, superGroups.size());
    }
}