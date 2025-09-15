package alluxio.master;

import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ServerConfiguration.class})
public class AlluxioMasterProcessConfigTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void verifyInvalidKeepAliveFailsFast() {
        // 1. Use the Alluxio 2.1.0 API to obtain configuration values via ServerConfiguration
        PowerMockito.mockStatic(ServerConfiguration.class);

        // 2. Prepare the test conditions: mock ServerConfiguration to return 0 ms
        when(ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE)).thenReturn(0L);
        when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM)).thenReturn(4);
        when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE)).thenReturn(4);
        when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE)).thenReturn(8);
        when(ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE)).thenReturn(1);

        // 3. Test code: attempt to create ForkJoinPool; expect IllegalArgumentException
        thrown.expect(IllegalArgumentException.class);

        // Directly trigger the ForkJoinPool constructor that is used in AlluxioMasterProcess
        new alluxio.concurrent.jsr.ForkJoinPool(
            ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_PARALLELISM),
            null,
            null,
            true,
            ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_CORE_POOL_SIZE),
            ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MAX_POOL_SIZE),
            ServerConfiguration.getInt(PropertyKey.MASTER_RPC_EXECUTOR_MIN_RUNNABLE),
            null,
            ServerConfiguration.getMs(PropertyKey.MASTER_RPC_EXECUTOR_KEEPALIVE),
            java.util.concurrent.TimeUnit.MILLISECONDS);

        // 4. No further verification needed – exception thrown above
    }
}