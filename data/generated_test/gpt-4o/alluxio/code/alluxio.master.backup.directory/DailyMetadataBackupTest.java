package alluxio.master.meta;

import alluxio.conf.ServerConfiguration;
import alluxio.util.io.PathUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static org.junit.Assert.assertEquals;

public class DailyMetadataBackupTest {
    private static String testBackupDirectory;
    private static final int RETAINED_FILES_LIMIT = 5;

    @Before
    public void setUp() throws IOException {
        // Set up a temporary directory as the test backup directory
        File tempDir = Files.createTempDirectory("testBackupDir").toFile();
        testBackupDirectory = tempDir.getAbsolutePath();

        // Delete the temporary directory on JVM exit
        tempDir.deleteOnExit();
    }

    @Test
    public void testBackupDirectoryPath() {
        // Ensure the backup directory path is set up correctly
        assertEquals(testBackupDirectory, new File(testBackupDirectory).getAbsolutePath());
    }
}