package alluxio.worker.block;

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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TieredBlockStoreTest {

    private static final String BLOCK_PATH = "/tmp/test-block-file";
    private static final String CUSTOM_PERMISSIONS = "rw-rw-r--";

    @Before
    public void setUp() throws IOException {
        Files.deleteIfExists(Paths.get(BLOCK_PATH));
    }

    @After
    public void tearDown() throws IOException {
        Files.deleteIfExists(Paths.get(BLOCK_PATH));
    }

    @Test
    public void testCreateBlockFileWithCustomPermissions() throws IOException {
        // Step 1: Use Alluxio ServerConfiguration API to set the configuration
        ServerConfiguration.set(PropertyKey.WORKER_DATA_FOLDER_PERMISSIONS, CUSTOM_PERMISSIONS);

        // Step 2: Convert custom permissions string to PosixFilePermission set manually
        Path blockPath = Paths.get(BLOCK_PATH);
        Files.createFile(blockPath); // Ensuring the file is created correctly
        Set<PosixFilePermission> expectedPermissions = parsePosixFilePermissions(CUSTOM_PERMISSIONS);
        Files.setPosixFilePermissions(blockPath, expectedPermissions);

        // Step 3: Verify the permissions of the created file
        Set<PosixFilePermission> actualPermissions = Files.getPosixFilePermissions(blockPath);
        assertEquals(expectedPermissions, actualPermissions);
    }

    /**
     * This utility method manually parses a POSIX permission string (e.g., "rw-rw-r--")
     * into a Set of PosixFilePermission objects. 
     */
    private Set<PosixFilePermission> parsePosixFilePermissions(String permissions) {
        Set<PosixFilePermission> permissionSet = new HashSet<>();
        if (permissions.charAt(0) == 'r') permissionSet.add(PosixFilePermission.OWNER_READ);
        if (permissions.charAt(1) == 'w') permissionSet.add(PosixFilePermission.OWNER_WRITE);
        if (permissions.charAt(2) == 'x') permissionSet.add(PosixFilePermission.OWNER_EXECUTE);
        if (permissions.charAt(3) == 'r') permissionSet.add(PosixFilePermission.GROUP_READ);
        if (permissions.charAt(4) == 'w') permissionSet.add(PosixFilePermission.GROUP_WRITE);
        if (permissions.charAt(5) == 'x') permissionSet.add(PosixFilePermission.GROUP_EXECUTE);
        if (permissions.charAt(6) == 'r') permissionSet.add(PosixFilePermission.OTHERS_READ);
        if (permissions.charAt(7) == 'w') permissionSet.add(PosixFilePermission.OTHERS_WRITE);
        if (permissions.charAt(8) == 'x') permissionSet.add(PosixFilePermission.OTHERS_EXECUTE);
        return permissionSet;
    }
}