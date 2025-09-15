package org.apache.hadoop.io.nativeio;   

import org.apache.hadoop.conf.Configuration;       
import org.apache.hadoop.io.nativeio.NativeIOException;       
import org.apache.hadoop.io.nativeio.NativeIO.POSIX.Stat;       
import org.apache.hadoop.io.nativeio.NativeIO;       
import org.junit.Assert;       
import org.junit.Test;       

import java.io.FileDescriptor;       
import java.io.IOException;

public class TestNativeIO {       
    @Test
    public void test_getFstat_with_invalid_file_descriptor() {
     
        Configuration conf = new Configuration();
        long cacheTimeout = conf.getLong(
            "hadoop.security.uid.cache.secs",
            14400
        );


        FileDescriptor invalidFileDescriptor = new FileDescriptor(); // Create an invalid file descriptor

        // Initialize NativeIO since it relies on native code and verify availability
        if (!NativeIO.isAvailable() || !NativeIO.POSIX.isAvailable()) {
            // This indicates that the native library is not correctly loaded. Modify the test to handle this scenario.
            System.out.println("NativeIO library is not available or unsupported on this platform.");
            return; // Skip the test if the library is not available
        }

        // 3. 测试代码
        try {
            // The native `getFstat` method might fail if the native libraries are not properly loaded; 
            // therefore, this test assumes that the native environment is properly set up.
            Stat stat = NativeIO.POSIX.getFstat(invalidFileDescriptor); // Call the method
            Assert.fail("Expected to throw a NativeIOException for invalid file descriptor");
        } catch (NativeIOException nioe) {
            // Verify the exception type and error message
            Assert.assertEquals("EBADF should cause error code 9", 9, nioe.getErrorCode()); // Error code for EBADF (invalid descriptor)
            Assert.assertTrue(nioe.getMessage().contains("bad file descriptor"));
        } catch (UnsatisfiedLinkError ule) {
            // If the native method is unavailable, log the issue
            Assert.fail("Native method is unavailable due to UnsatisfiedLinkError");
        } catch (IOException e) {
            Assert.fail("Expected NativeIOException, but received IOException");
        }

    }
}