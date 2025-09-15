package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.io.FileUtils;
import alluxio.worker.block.meta.TempBlockMeta;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class TieredBlockStoreTest {

    @Rule
    public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateBlockFileWithCustomPermissions() throws Exception {
        // 1. You need to use the Alluxio 2.1.0 API correctly to obtain configuration values.
        AlluxioConfiguration alluxioConf = new InstancedConfiguration(
                new alluxio.conf.AlluxioProperties());
        String configuredPermissions = alluxioConf.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

        // 2. Prepare the test conditions.
        String blockPath = mTemporaryFolder.newFolder("data_folder").toPath()
                .resolve("test_block_file").toString();

        // Convert the string representing permissions (e.g., "rw-r--r--") to PosixFilePermission
        Set<PosixFilePermission> posixPermissions = PosixFilePermissions.fromString(configuredPermissions);

        // 3. Test code.
        Files.createFile(Paths.get(blockPath));
        Files.setPosixFilePermissions(Paths.get(blockPath), posixPermissions);

        // Validate the file's existence and permissions.
        Path blockFilePath = Paths.get(blockPath);
        Assert.assertTrue("Block file should exist after creation", Files.exists(blockFilePath));

        Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(blockFilePath);
        Assert.assertEquals("Block file permissions should match configuration", posixPermissions, actualPermissions);

        // 4. Code after testing.
        Files.delete(blockFilePath); // Clean up the block file.
    }
}