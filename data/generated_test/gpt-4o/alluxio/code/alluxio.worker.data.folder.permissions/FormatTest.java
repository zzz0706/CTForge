package alluxio.cli;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class FormatTest {

    @Test
    public void testFormatWorkerDataFolderWithDefaultPermissions() throws IOException {
        // 1. Obtain the configuration value for worker data folder permissions.
        String permissionsConfig = ServerConfiguration.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);
        if (permissionsConfig == null) {
            permissionsConfig = "rwxr-xr-x"; // Fallback to default if config is null
        }
        Set<PosixFilePermission> defaultPermissions = PosixFilePermissions.fromString(permissionsConfig);

        // 2. Prepare the test conditions: Create a test folder
        String testFolderPath = "/tmp/test_worker_data_folder";
        Path testFolder = Paths.get(testFolderPath);
        if (Files.exists(testFolder)) {
            FileUtils.deleteDirectory(testFolder.toFile()); // Clean up any previous test folder
        }
        Files.createDirectory(testFolder);

        // 3. Apply the default permissions to the folder
        Files.setPosixFilePermissions(testFolder, defaultPermissions);

        // 4. Assertions: Verify the folder is created with the correct permissions
        Assert.assertTrue(Files.exists(testFolder));
        Assert.assertEquals(defaultPermissions, Files.getPosixFilePermissions(testFolder));

        // 5. Cleanup after the test
        FileUtils.deleteDirectory(testFolder.toFile());
    }
}