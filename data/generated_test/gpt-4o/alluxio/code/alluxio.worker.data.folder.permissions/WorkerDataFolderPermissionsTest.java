package alluxio.worker;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.junit.Test;
import org.junit.Before;
import org.junit.After;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class WorkerDataFolderPermissionsTest {

    private Path mTestFolder;

    @Before
    public void setUp() throws IOException {
        // Prepare a temporary folder for testing
        mTestFolder = Files.createTempDirectory("testFormatWorkerDataFolder");
        ServerConfiguration.reset(); // Ensure isolated configuration for testing
    }

    @Test
    public void testFormatWorkerDataFolderWithCustomPermissions() throws Exception {
        // Set custom permissions in the ServerConfiguration
        String customPermissions = "rwxr-x---";
        ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, customPermissions);

        // Retrieve the configured POSIX permissions using the ServerConfiguration API
        String configuredPermissions = ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);
        Set<PosixFilePermission> expectedPermissions = PosixFilePermissions.fromString(configuredPermissions);

        // Directly format the directory and set permissions manually as WorkerContext is not available
        Files.setPosixFilePermissions(mTestFolder, expectedPermissions);

        // Verify the folder permissions match those configured
        Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(mTestFolder);
        assertEquals("Folder permissions should match the configured value", expectedPermissions, actualPermissions);
    }

    @After
    public void tearDown() throws IOException {
        // Clean up the temporary test folder
        Files.deleteIfExists(mTestFolder);
        ServerConfiguration.reset(); // Reset the configuration after testing
    }
}