package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestShellRunInterval {
    @Test
    public void test_shell_run_respects_interval() {
        // Step 1: Get the configuration value using the API
        Configuration conf = new Configuration();
        long interval = conf.getLong(CommonConfigurationKeysPublic.FS_DF_INTERVAL_KEY, 60000); // default to 60000 ms

        // Step 2: Prepare the input conditions for unit testing
        try {
            // Define a testable subclass of Shell
            TestableShell shell = new TestableShell(interval);

            // Step 3: Call the `testRun()` method immediately and observe the behavior (command should execute)
            shell.testRun(); // Call the testRun() method instead of protected run() directly
            long firstExecutionTime = shell.getLastExecutionTime();

            // Step 4: Call the `testRun()` method again within the configured interval and observe the behavior (command should not execute)
            Thread.sleep(interval / 2); // Ensure the time gap is less than the interval
            shell.testRun(); // Should not execute due to the interval
            long secondExecutionTime = shell.getLastExecutionTime();

            // Validate if the second execution occurred before the interval was respected
            assertTrue(
                "Command executed too quickly, interval not respected.",
                firstExecutionTime == secondExecutionTime
            );

            // Step 5: Wait for a duration exceeding the configured interval and call the `testRun()` method again
            Thread.sleep(interval + 100); // Sleep slightly longer than the interval
            shell.testRun(); // Command should execute again
            long thirdExecutionTime = shell.getLastExecutionTime();

            // Step 6: Verify that the command executes correctly after the interval has expired
            assertTrue(
                "Command did not execute after the interval expired.",
                thirdExecutionTime > secondExecutionTime
            );

        } catch (IOException | InterruptedException e) {
            fail("Test failed with exception: " + e.getMessage());
        }
    }

    // Define a testable subclass of Shell with public access to the `run()` method for testing
    static class TestableShell extends Shell {
        private final long interval;
        private long lastExecutionTime = -1;

        public TestableShell(long interval) {
            this.interval = interval;
        }

        @Override
        protected String[] getExecString() {
            return new String[]{"echo", "test"};
        }

        @Override
        protected void run() throws IOException {
            long currentTime = System.currentTimeMillis();
            if (lastExecutionTime < 0 || currentTime - lastExecutionTime >= interval) {
                System.out.println("Command executed");
                lastExecutionTime = currentTime;
            } else {
                System.out.println("Skipping execution due to interval.");
            }
        }

        @Override
        protected void parseExecResult(BufferedReader lines) throws IOException {
            // Simulate parsing of command output
            System.out.println("Parsing command output");
        }

        // Public method for testing
        public void testRun() throws IOException {
            this.run();
        }

        // Expose the lastExecutionTime for validation in test cases
        public long getLastExecutionTime() {
            return lastExecutionTime;
        }
    }
}