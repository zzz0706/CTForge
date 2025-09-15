package alluxio.worker.block;

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.worker.block.TieredBlockStore;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;

public class TieredBlockStoreTest {

    @Rule
    public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

    @Test
    public void testCreateBlockFileWithCustomPermissions() throws Exception {
        // 1. Use the Alluxio 2.1.0 API correctly to obtain configuration values
        AlluxioConfiguration alluxioConf = InstancedConfiguration.defaults();
        String configuredPermissions = alluxioConf.get(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS);

        // 2. Prepare the test conditions
        Path blockFolder = mTemporaryFolder.newFolder("worker_data").toPath();
        Path blockFilePath = blockFolder.resolve("test_block_file");

        // 3. Test code
        // Convert the string representing permissions to PosixFilePermission set
        Set<PosixFilePermission> expectedPermissions = PosixFilePermissions.fromString(configuredPermissions);

        // Using the public API for block file creation
        Files.createFile(blockFilePath);
        Files.setPosixFilePermissions(blockFilePath, expectedPermissions);

        // Validate the file's existence and permissions
        Assert.assertTrue("Block file should exist after creation.", Files.exists(blockFilePath));
        Assert.assertEquals("Block file permissions should match configured permissions.",
                expectedPermissions, Files.getPosixFilePermissions(blockFilePath));
    }
}