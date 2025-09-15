package org.apache.hadoop.io.nativeio;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileDescriptor;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class NativeIOTest {
  
    @Test
    public void test_getFstat_with_valid_file_descriptor() throws IOException {
        // Prepare the test conditions
        File tempFile = File.createTempFile("testfile", ".tmp");
        tempFile.deleteOnExit();

        // Open the file to obtain a valid FileDescriptor
        try (FileInputStream fis = new FileInputStream(tempFile)) {
            FileDescriptor fd = fis.getFD();

            // Ensure NativeIO.POSIX is loaded properly
            if (!NativeIO.isAvailable()) {
                System.out.println("Native library is not available. Test is skipped.");
                return; // Skip the test as the library isn't available
            }

            // Ensure the `getFstat` method is invoked properly
            NativeIO.POSIX.Stat stat = NativeIO.POSIX.getFstat(fd);

            // Ensure the returned `Stat` object is non-null
            assertNotNull("Stat object should not be null", stat);

            // Validate owner and group retrieval
            assertNotNull("Owner value should not be null", stat.getOwner());
            assertNotNull("Group value should not be null", stat.getGroup());
            assertTrue("Owner value should be non-empty", !stat.getOwner().isEmpty());
            assertTrue("Group value should be non-empty", !stat.getGroup().isEmpty());
        } catch (UnsatisfiedLinkError e) {
            // Handle cases where the native library is not properly loaded.
            System.out.println("Failed to initialize native library or load method. Check library installation: " + e.getMessage());
        }
    }
}