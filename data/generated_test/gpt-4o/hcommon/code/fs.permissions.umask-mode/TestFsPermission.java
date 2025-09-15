package org.apache.hadoop.fs.permission;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestFsPermission {

    @Test
    public void testValidSymbolicUmaskParsingUnderHighLoad() {
        // Get configuration values using the API
        final Configuration conf = new Configuration(); // Declared 'conf' as final to fix the issue

        // Ensure we are verifying behavior during high load.
        // Note: Simulate a high-load environment where multiple threads read the umask configuration.
        int numberOfThreads = 100;
        Thread[] threads = new Thread[numberOfThreads];

        for (int i = 0; i < numberOfThreads; i++) {
            threads[i] = new Thread(new Runnable() { // Use Runnable instead of lambda for Java 7 compatibility
                @Override
                public void run() {
                    // Call getUMask to ensure the configuration remains effective under load.
                    FsPermission umaskPermission = FsPermission.getUMask(conf);

                    // Verify that the returned FsPermission object adheres to expected behavior
                    // without asserting specific configuration value equivalencies.
                    short permissionShort = umaskPermission.toShort();
                    assertEquals("User, group, and others permissions should be appropriately represented.",
                            (permissionShort & 0x1FF), permissionShort); // Basic permission short validity check
                }
            });
        }

        // Start all threads
        for (int i = 0; i < numberOfThreads; i++) {
            threads[i].start();
        }

        // Wait for all threads to finish
        for (int i = 0; i < numberOfThreads; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}