package alluxio.master.meta;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.underfs.UfsManager;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UfsStatus;
import alluxio.resource.CloseableResource;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.lang.reflect.Method;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class DailyMetadataBackupTest {

    @Test
    public void verifyDeleteStaleBackupsListsCorrectDirectory() throws Exception {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values
        String expectedDir = ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY);

        // 2. Prepare the test conditions
        UfsManager ufsManager = mock(UfsManager.class);
        UfsManager.UfsClient ufsClient = mock(UfsManager.UfsClient.class);
        UnderFileSystem ufs = mock(UnderFileSystem.class);

        when(ufsManager.getRoot()).thenReturn(ufsClient);
        when(ufsClient.acquireUfsResource()).thenReturn(new CloseableResource<UnderFileSystem>(ufs) {
            @Override
            public void close() {}
        });
        when(ufs.getUnderFSType()).thenReturn("local");

        UfsStatus[] statuses = new UfsStatus[2];
        statuses[0] = mock(UfsStatus.class);
        statuses[1] = mock(UfsStatus.class);
        when(statuses[0].isFile()).thenReturn(true);
        when(statuses[1].isFile()).thenReturn(true);
        when(statuses[0].getName()).thenReturn("alluxio-backup-2023-01-01-1672531200000.gz");
        when(statuses[1].getName()).thenReturn("alluxio-backup-2023-01-02-1672617600000.gz");
        when(ufs.listStatus(anyString())).thenReturn(statuses);

        PowerMockito.mockStatic(ServerConfiguration.class);
        when(ServerConfiguration.get(PropertyKey.MASTER_BACKUP_DIRECTORY)).thenReturn(expectedDir);
        when(ServerConfiguration.getInt(PropertyKey.MASTER_DAILY_BACKUP_FILES_RETAINED)).thenReturn(1);

        MetaMaster metaMaster = mock(MetaMaster.class);
        java.util.concurrent.ScheduledExecutorService service = mock(java.util.concurrent.ScheduledExecutorService.class);
        DailyMetadataBackup dailyMetadataBackup = new DailyMetadataBackup(metaMaster, service, ufsManager);

        // 3. Test code
        Method method = DailyMetadataBackup.class.getDeclaredMethod("deleteStaleBackups");
        method.setAccessible(true);
        method.invoke(dailyMetadataBackup);

        // 4. Code after testing
        verify(ufs).listStatus(expectedDir);
    }
}