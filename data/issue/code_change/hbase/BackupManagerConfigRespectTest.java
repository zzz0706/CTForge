package org.apache.hadoop.hbase.backup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.backup.impl.BackupManager;
import org.apache.hadoop.hbase.master.cleaner.HFileCleaner;
import org.apache.hadoop.hbase.master.cleaner.HFileLinkCleaner;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
//HBASE-19258
public class BackupManagerConfigRespectTest {

  private static final String KEY = HFileCleaner.MASTER_HFILE_CLEANER_PLUGINS;
  private static final String SNAPSHOT = SnapshotHFileCleaner.class.getName();
  private static final String LINK = HFileLinkCleaner.class.getName();
  private static final String BACKUP = "org.apache.hadoop.hbase.backup.BackupHFileCleaner";

  private static String[] rawTokens(String s) {
    if (s == null) return new String[0];
    return s.split(",");
  }

  private static String[] trimmedTokens(String s) {
    return Arrays.stream(rawTokens(s)).map(String::trim).toArray(String[]::new);
  }

  @Test
  public void userConfiguredListShouldRemainUnchanged() {
    Configuration conf = HBaseConfiguration.create();
    String userValue = SNAPSHOT + "," + LINK; 
    conf.set(KEY, userValue);

    BackupManager.decorateMasterConfiguration(conf);

    String after = conf.get(KEY);
    assertEquals(
        "Must respect user's plugins list; should not force-append BackupHFileCleaner",
        userValue, after);
  }

  @Test
  public void shouldNotDuplicateBackupCleanerIfAlreadyPresent() {
    Configuration conf = HBaseConfiguration.create();
    String userValue = SNAPSHOT + "," + BACKUP + "," + LINK;
    conf.set(KEY, userValue);

    BackupManager.decorateMasterConfiguration(conf);

    long count = Arrays.stream(trimmedTokens(conf.get(KEY)))
        .filter(BACKUP::equals)
        .count();
    assertEquals("BackupHFileCleaner must appear at most once", 1, count);
  }


  @Test
  public void emptyValueShouldNotCreateEmptyTokenOnDecoration() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(KEY, ""); 

    BackupManager.decorateMasterConfiguration(conf);

    String[] tokens = rawTokens(conf.get(KEY));
    for (String t : tokens) {
      assertFalse("No empty plugin name token should be introduced", t.isEmpty());
    }
  }
}
