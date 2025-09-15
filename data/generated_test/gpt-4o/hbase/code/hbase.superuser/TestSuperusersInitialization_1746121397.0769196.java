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
        // 1. Use the HBase 2.2.2 API to correctly obtain configuration values.
        Configuration conf = new Configuration();
        conf.set(Superusers.SUPERUSER_CONF_KEY, "user1,user2,@group1");

        // 2. Prepare the test conditions.
        // Ensure no prior initialization of Superusers.
        Superusers.initialize(new Configuration());

        // 3. Test code: Concurrent initialization.
        ExecutorService executorService = Executors.newFixedThreadPool(10);

        for (int i = 0; i < 10; i++) {
            executorService.submit(() -> {
                try {
                    Superusers.initialize(conf);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        // Wait until all threads finish execution.
        executorService.shutdown();
        executorService.awaitTermination(60, TimeUnit.SECONDS);

        // 4. Code after testing: Verify the results using assertions.
        java.util.Collection<String> superUsersList = Superusers.getSuperUsers();
        java.util.Collection<String> superGroupsList = Superusers.getSuperGroups();

        // Validate that superusers and supergroups contain expected entries.
        assertTrue("Expected superUser 'user1' not found", superUsersList.contains("user1"));
        assertTrue("Expected superUser 'user2' not found", superUsersList.contains("user2"));
        assertTrue("Expected superGroup '@group1' not found", superGroupsList.contains("@group1"));
    }
}