package uk.co.odinconsultants.arrow;

import com.google.flatbuffers.LongVector;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.IntVector;

import java.io.FileOutputStream;
import java.io.OutputStream;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class PingMain {

    public static final int MAX_ALLOCATION = 1024 * 1024;
    public static final String PING_FILE = "/dev/shm/ping";

    public static void main(String[] args) throws Exception {
        try (final RootAllocator allocator =
                     new RootAllocator(MAX_ALLOCATION)) {
            IntVector intVector = new IntVector("int", allocator);
            intVector.setSafe(0, 42);
            intVector.setValueCount(1);
            List<Field> fields = Arrays.asList(intVector.getField());
            List<FieldVector> vectors = Arrays.asList(intVector);
            VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);

            try (
                    OutputStream out = new FileOutputStream(PING_FILE);
                    ArrowStreamWriter writer = new ArrowStreamWriter(root, null, Channels.newChannel(out));
            ) {
                writer.start();

                for (int i = 0; i < 4; i++) {
                    writer.writeBatch();
                    // populate VectorSchemaRoot data and write the second batch
                    IntVector childVector1 = (IntVector)root.getVector(0);
                    int old = childVector1.get(0);
                    System.out.println(old);
                    childVector1.reset();
                    childVector1.setSafe(0, old + 1);

                    writer.end();
                    writer.start();
                }

                writer.end();
                out.flush();
            }

            root.close();
            System.out.println("Finished");
        }
    }

}
