package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class MasterBackupDirectoryConfigValidationTest {

  private static final PropertyKey BACKUP_DIR_KEY = PropertyKey.MASTER_BACKUP_DIRECTORY;

  @Before
  public void before() {
    // Ensure the backup directory is set to an absolute path for the test
    ServerConfiguration.set(BACKUP_DIR_KEY, "/tmp/alluxio_backups");
  }

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void backupDirectoryIsAbsolutePath() {
    String dir = ServerConfiguration.get(BACKUP_DIR_KEY);
    assertTrue("Backup directory must be absolute: " + dir,
        new File(dir).isAbsolute());
  }

  @Test
  public void backupDirectoryIsNotEmpty() {
    String dir = ServerConfiguration.get(BACKUP_DIR_KEY);
    assertTrue("Backup directory must not be empty", dir != null && !dir.trim().isEmpty());
  }

  @Test
  public void backupDirectoryDoesNotEndWithSeparator() {
    String dir = ServerConfiguration.get(BACKUP_DIR_KEY);
    assertTrue("Backup directory must not end with separator",
        !dir.endsWith("/"));
  }

  @Test
  public void backupDirectoryContainsNoRelativePathComponents() {
    String dir = ServerConfiguration.get(BACKUP_DIR_KEY);
    assertTrue("Backup directory must not contain relative path components",
        !dir.contains("/../") && !dir.endsWith("/.."));
  }
}