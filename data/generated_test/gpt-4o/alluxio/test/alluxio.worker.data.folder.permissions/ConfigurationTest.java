package alluxio.cli;

import alluxio.conf.AlluxioConfiguration;
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

public class ConfigurationTest {

    private Path testWorkerFolder;

    @Before
    public void setUp() throws IOException {
        // Prepare the test conditions
        testWorkerFolder = Paths.get(System.getProperty("java.io.tmpdir"), "testWorkerFolder");
        if (Files.exists(testWorkerFolder)) {
            Files.walk(testWorkerFolder)
                .map(Path::toFile)
                .forEach(file -> {
                    if (!file.delete()) {
                        file.deleteOnExit();
                    }
                }); // Ensure the folder does not exist
        }
    }

    @After
    public void tearDown() throws IOException {
        // Clean up after testing
        if (Files.exists(testWorkerFolder)) {
            Files.walk(testWorkerFolder)
                .map(Path::toFile)
                .forEach(file -> {
                    if (!file.delete()) {
                        file.deleteOnExit();
                    }
                });
        }
    }

    @Test
    public void testFormatWorkerDataFolderWithCustomPermissions() throws IOException {
        // Obtain configuration value from Alluxio and set the expected permissions
        String customPermissions = "rwxr-xr--";
        ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, customPermissions);

        // Prepare Alluxio configuration
        AlluxioConfiguration alluxioConf = ServerConfiguration.global();

        // Format the worker's data folder by creating the directory and setting proper permissions
        Files.createDirectories(testWorkerFolder);
        Files.setPosixFilePermissions(testWorkerFolder, PosixFilePermissions.fromString(customPermissions));

        // Verify the folder creation with specified permissions
        assertTrue(Files.exists(testWorkerFolder));
        Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(testWorkerFolder);
        Set<PosixFilePermission> expectedPermissions = PosixFilePermissions.fromString(customPermissions);
        assertEquals(expectedPermissions, actualPermissions);

        // Check the folder is readable and writable
        assertTrue(Files.isWritable(testWorkerFolder) && Files.isReadable(testWorkerFolder));
    }
}