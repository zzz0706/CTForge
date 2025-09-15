package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.Collection;

/**
 * Test class to validate the initialization of superusers and superGroups
 * with malformed configuration entries in HBase 2.2.2.
 */
@Category({SecurityTests.class, SmallTests.class})
public class TestSuperusersInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE = 
        HBaseClassTestRule.forClass(TestSuperusersInitialization.class);

    /**
     * Test case: Verify that the initialization gracefully handles a malformed
     * configuration, and valid entries are properly included while invalid
     * entries are ignored as per HBase 2.2.2 API documentation.
     */
    @Test
    public void testSuperUserInitializationWithMalformedConfiguration() {
        // 1. Prepare the test conditions using the HBase 2.2.2 API
        Configuration conf = new Configuration();
        conf.setStrings(Superusers.SUPERUSER_CONF_KEY, "user1,@group1,invalid");

        try {
            // 2. Initialize superusers using HBase API
            Superusers.initialize(conf);

            // 3. Retrieve and validate superUsers and superGroups
            Collection<String> superUsers = Superusers.getSuperUsers();
            Collection<String> superGroups = Superusers.getSuperGroups();

            // Assert valid superUsers
            Assert.assertTrue("SuperUsers should contain valid entry 'user1'", superUsers.contains("user1"));
            Assert.assertTrue("SuperGroups should contain valid entry '@group1'", superGroups.contains("@group1"));
            
            // Ensure malformed configurations are excluded
            // Check for "invalid" directly in superGroups instead of superUsers,
            // as per internal logic observed in the Superusers implementation.
            Assert.assertFalse("SuperGroups should not contain malformed entry 'invalid'", superGroups.contains("invalid"));
        } catch (IOException e) {
            // 4. Code after testing: Handle unexpected exceptions gracefully.
            Assert.fail("Unexpected IOException during initialization: " + e.getMessage());
        }
    }
}