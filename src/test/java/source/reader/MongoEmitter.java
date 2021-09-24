package source.reader;

import mongoflink.serde.DocumentDeserializer;
import org.apache.flink.api.connector.source.SourceOutput;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.bson.Document;
import source.split.FlinkSplitState;

/**
 * A {@link RecordEmitter} that transforms a Mongo document into records fo the required type.
 **/
public class MongoEmitter<E> implements RecordEmitter<Document, E, FlinkSplitState> {

    private final DocumentDeserializer<E> deserializer;

    MongoEmitter(DocumentDeserializer<E> deserializer) {
        this.deserializer = deserializer;
    }

    @Override
    public void emitRecord(Document element, SourceOutput<E> output, FlinkSplitState splitState) throws Exception {
        output.collect(deserializer.deserialize(element));
        splitState.increaseOffset(1);
    }
}
