package io.github.MonsterChenzhuo.source.reader;

import io.github.MonsterChenzhuo.internal.connection.MongoClientProvider;
import io.github.MonsterChenzhuo.serde.DocumentDeserializer;
import io.github.MonsterChenzhuo.source.split.MongoSplit;
import io.github.MonsterChenzhuo.source.split.MongoSplitState;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.bson.Document;

import java.util.Map;

/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * MongoReader reads MongoDB by splits (queries).
 **/
public class MongoReader<E> extends SingleThreadMultiplexSourceReaderBase<Document, E, MongoSplit, MongoSplitState> {

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
    protected MongoSplitState initializedState(MongoSplit split) {
        return new MongoSplitState(split);
    }

    @Override
    protected MongoSplit toSplitType(String splitId, MongoSplitState splitState) {
        return new MongoSplit(splitId, splitState.getQuery(), splitState.getCurrentOffset());
    }

    @Override
    protected void onSplitFinished(Map<String, MongoSplitState> map) {
        context.sendSplitRequest();
    }
}
