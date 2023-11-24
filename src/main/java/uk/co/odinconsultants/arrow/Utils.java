package uk.co.odinconsultants.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileReader;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.ipc.message.ArrowBlock;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.List;

public class Utils {

    static void writeFile(VectorSchemaRoot root, String filename) throws IOException {
        try (final RandomAccessFile raf = new RandomAccessFile(filename, "rw");
             final FileChannel channel = raf.getChannel();
             final ArrowFileWriter writer =
                     new ArrowFileWriter(root,
                             null,
                             channel
//                             new HashMap<>(),
//                             IpcOption.DEFAULT,
//                             CommonsCompressionFactory.INSTANCE,
//                             CompressionUtil.CodecType.ZSTD,
//                             Optional.of(7)
                     )) {
            writer.start();
            writer.writeBatch();
            writer.end();
        }
    }

    static VectorSchemaRoot readFile(RootAllocator allocator, String filename) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(filename, "rw");
             FileChannel channel = raf.getChannel();
             ArrowFileReader reader =
                     new ArrowFileReader(channel,
                             allocator
//                             CommonsCompressionFactory.INSTANCE
                     )) {
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
