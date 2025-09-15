package alluxio.cli;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

import static org.junit.Assert.*;

public class FormatTest {
    private Path testFolderPath;

    @Before
    public void setUp() throws IOException {
        // Prepare the test conditions
        testFolderPath = Paths.get(System.getProperty("java.io.tmpdir"), "testWorkerFolder");
        Files.deleteIfExists(testFolderPath); // Ensure the folder does not exist
    }

    @After
    public void tearDown() throws IOException {
        // Clean up after testing
        Files.deleteIfExists(testFolderPath);
    }

    @Test
    public void testFormatWorkerDataFolderWithCustomPermissions() throws IOException {
        // Set the configuration for the test
        String customPermissions = "rwxr-xr--";
        ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, customPermissions);

        // Call the method to test via reflection since the method is private
        try {
            java.lang.reflect.Method method = Format.class.getDeclaredMethod("formatWorkerDataFolder", String.class);
            method.setAccessible(true); // Make the private method accessible
            method.invoke(null, testFolderPath.toAbsolutePath().toString());
        } catch (Exception e) {
            fail("Failed to invoke the private method: " + e.getMessage());
        }

        // Verify the folder is created with the specified permissions
        assertTrue(Files.exists(testFolderPath));
        Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(testFolderPath);
        Set<PosixFilePermission> expectedPermissions = PosixFilePermissions.fromString(customPermissions);
        assertEquals(expectedPermissions, actualPermissions);

        // Check the folder is readable and writable
        assertTrue(Files.isWritable(testFolderPath) && Files.isReadable(testFolderPath));
    }
}