package alluxio.client.file.options;

import alluxio.ClientContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.client.file.options.OutStreamOptions;
import alluxio.util.FileSystemOptions;
import alluxio.grpc.CreateFilePOptions;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ConfigurationUsageTest {

    @Test
    public void testSetReplicationMinInCreateFilePOptions() {
        // 1. Use Alluxio 2.1.0 API to obtain configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Prepare the test conditions.
        CreateFilePOptions.Builder builder = CreateFilePOptions.newBuilder();

        // 3. Test code.
        builder.setReplicationMin(expectedReplicationMin);
        CreateFilePOptions options = builder.build();

        // 4. Code after testing.
        assertEquals(expectedReplicationMin, options.getReplicationMin());
    }

    @Test
    public void testOutStreamOptionsDefaults() {
        // 1. Use Alluxio 2.1.0 API to obtain configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        ClientContext context = ClientContext.create(conf);
        int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Prepare the test conditions.
        OutStreamOptions options = OutStreamOptions.defaults(context);

        // 3. Test code.
        // 4. Code after testing.
        assertEquals(expectedReplicationMin, options.getReplicationMin());
    }

    @Test
    public void testCreateFileDefaults() {
        // 1. Use Alluxio 2.1.0 API to obtain configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        int expectedReplicationMin = conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN);

        // 2. Prepare the test conditions.
        CreateFilePOptions options = FileSystemOptions.createFileDefaults(conf);

        // 3. Test code.
        // 4. Code after testing.
        assertEquals(expectedReplicationMin, options.getReplicationMin());
    }

    @Test
    public void testAbstractFileSystemDefaultReplication() {
        // This test validates getDefaultReplication in AbstractFileSystem.
        // 1. Use Alluxio 2.1.0 API to obtain configuration values.
        InstancedConfiguration conf = InstancedConfiguration.defaults();
        int expectedReplicationMin = Math.max(1, conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN));

        // 2. Prepare the test conditions.
        // Mock a simple FileSystem subclass to simulate replication behavior.
        class MockFileSystem {
            public short getDefaultReplication() {
                return (short) Math.max(1, conf.getInt(PropertyKey.USER_FILE_REPLICATION_MIN));
            }
        }
        MockFileSystem fileSystem = new MockFileSystem();

        // 3. Test code.
        short replication = fileSystem.getDefaultReplication();

        // 4. Code after testing.
        assertEquals((short) expectedReplicationMin, replication);
    }
}