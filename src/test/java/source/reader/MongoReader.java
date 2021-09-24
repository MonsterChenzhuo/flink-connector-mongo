package source.reader;

import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.serde.DocumentDeserializer;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.bson.Document;
import source.split.MongoSplit;
import source.split.FlinkSplitState;

import java.util.Map;

/**
 * MongoReader reads MongoDB by splits (queries).
 **/
public class MongoReader<E> extends SingleThreadMultiplexSourceReaderBase<Document, E, MongoSplit, FlinkSplitState> {

    public MongoReader(SourceReaderContext context,
                       MongoClientProvider clientProvider,
                       DocumentDeserializer<E> deserializer) {
        super(
                () -> new MongoSplitReader(clientProvider),
                new MongoEmitter<>(deserializer),
                context.getConfiguration(),
                context
                );
    }

    @Override
    public void start() {
        context.sendSplitRequest();
    }

    @Override
    protected FlinkSplitState initializedState(MongoSplit split) {
        return new FlinkSplitState(split);
    }

    @Override
    protected MongoSplit toSplitType(String splitId, FlinkSplitState splitState) {
        return new MongoSplit(splitId, splitState.getQuery(), splitState.getCurrentOffset());
    }

    @Override
    protected void onSplitFinished(Map<String, FlinkSplitState> map) {
        context.sendSplitRequest();
    }
}
