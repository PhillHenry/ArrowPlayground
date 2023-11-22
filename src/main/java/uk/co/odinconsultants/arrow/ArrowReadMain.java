package uk.co.odinconsultants.arrow;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileInputStream;

import static uk.co.odinconsultants.arrow.ArrowWriteMain.MAX_ALLOCATION;
import static uk.co.odinconsultants.arrow.ArrowWriteMain.SHM_FILE;

public class ArrowReadMain {

    public static void main(String[] args) throws Exception {
        try (
                final RootAllocator allocator =
                        new RootAllocator(MAX_ALLOCATION);
                ArrowStreamReader reader = new ArrowStreamReader(new FileInputStream(SHM_FILE), allocator)) {
            // This will be loaded with new values on every call to loadNextBatch
            VectorSchemaRoot readRoot = reader.getVectorSchemaRoot();
            Schema schema = readRoot.getSchema();
            reader.loadNextBatch();
            // ... do something with readRoot
            // get the encoded vector
            BitVector bitVector = (BitVector) readRoot.getVector(0);
            System.out.println(bitVector);

            VarCharVector childVector2 = (VarCharVector)readRoot.getVector(1);
            System.out.println(childVector2);
        }
    }

}
