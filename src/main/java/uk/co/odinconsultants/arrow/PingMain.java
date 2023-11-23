package uk.co.odinconsultants.arrow;

import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.GenerateSampleData;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class PingMain {

    public static final int MAX_ALLOCATION = 1024 * 1024;
    public static final String PING_FILE = "/dev/shm/ping";

    public static void main(String[] args) throws Exception {
        try (final RootAllocator allocator =
                     new RootAllocator(Integer.MAX_VALUE)) {
            List<Field> fields = new ArrayList<>();
            fields.add(new IntVector("int", allocator).getField());

            VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
            final int rowCount = 1;
            GenerateSampleData.generateTestData(root.getVector(0), rowCount);

            IntVector intVector = (IntVector)root.getVector(0);
            intVector.setSafe(0, 42);
            intVector.setValueCount(1);

            write(root);
            root.close();
//            root.close();
            VectorSchemaRoot newRoot = read(allocator);
            newRoot.close();

            System.out.println("Finished");
        }
    }

    static void write(VectorSchemaRoot root) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(PING_FILE, "rw");
             final FileChannel channel = raf.getChannel();
                final ArrowFileWriter writer =
                     new ArrowFileWriter(root,
                             null,
                             channel,
                             new HashMap<>(),
                             IpcOption.DEFAULT,
                             CommonsCompressionFactory.INSTANCE,
                             CompressionUtil.CodecType.ZSTD,
                             Optional.of(7))) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    static VectorSchemaRoot read(RootAllocator allocator) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(PING_FILE, "rw");
             FileChannel channel = raf.getChannel();
             ArrowFileReader reader =
                     new ArrowFileReader(channel,
                             allocator,
                             CommonsCompressionFactory.INSTANCE)) {
            List<ArrowBlock> recordBlocks = reader.getRecordBlocks();
            ArrowBlock block = recordBlocks.get(0);
            reader.loadRecordBatch(block);
            VectorSchemaRoot newRootX =  reader.getVectorSchemaRoot();
            FieldVector childVector = newRootX.getVector(0);
            assert(childVector != null);
            int old = (int) childVector.getObject(0);
            System.out.println(old);
            childVector.close();
            return newRootX;
        }
    }

}
