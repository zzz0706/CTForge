package org.apache.hadoop.hbase.security;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

@Category({SecurityTests.class, SmallTests.class})
public class TestSuperusersInitialization {

    @ClassRule
    public static final HBaseClassTestRule CLASS_RULE =
        HBaseClassTestRule.forClass(TestSuperusersInitialization.class);

    @Test
    public void test_superUserInitialization_withConcurrentExecution() throws IOException, InterruptedException {
        // 1. You need to use the HBase 2.2.2 API correctly to obtain configuration values, instead of hardcoding the configuration values.
        Configuration conf = new Configuration();
        conf.set(Superusers.SUPERUSER_CONF_KEY, "user1,user2,@group1");

        // 2. Prepare the test conditions.
        // Initialize with a clean configuration to ensure no prior data in Superusers.
        Superusers.initialize(new Configuration());

        // 3. Test code: Concurrent initialization using multiple threads.
        ExecutorService executorService = Executors.newFixedThreadPool(10);
        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    Superusers.initialize(conf);
                } catch (IOException e) {
                    throw new RuntimeException("Error during Superusers initialization in thread", e);
                }
            });
        }

        // Ensure all threads finish execution within a timeout.
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        // 4. Code after testing: Verify the results using assertions.
        // Retrieve and validate superusers and supergroups.
        java.util.Collection<String> superUsersList = Superusers.getSuperUsers();
        java.util.Collection<String> superGroupsList = Superusers.getSuperGroups();

        // Assertions to validate correctness of superuser and supergroup initialization.
        assertTrue("Expected superUser 'user1' not found", superUsersList.contains("user1"));
        assertTrue("Expected superUser 'user2' not found", superUsersList.contains("user2"));
        assertTrue("Expected superGroup '@group1' not found", superGroupsList.contains("@group1"));
        
        // Ensure system user is included in the superUsers set.
        String currentUser = User.getCurrent().getShortName();
        assertTrue("Expected system user '" + currentUser + "' not found", superUsersList.contains(currentUser));
    }

}