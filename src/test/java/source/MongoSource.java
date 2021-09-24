package source;

import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.serde.DocumentDeserializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import source.enumerator.MongoSplitEnumerator;
import source.reader.MongoReader;
import source.split.FileSplit;
import source.split.ListMongoSplitSerializer;
import source.split.MongoSplitSerializer;
import source.split.MongoSplitStrategy;

import java.util.List;

/**
 * MongoSource for bounded scenarios.
 **/
public class MongoSource<T> implements Source<T, FileSplit, List<FileSplit>> {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<T> deserializer;

    private MongoSplitStrategy splitStrategy;

    public MongoSource(MongoClientProvider clientProvider,
                       DocumentDeserializer<T> deserializer,
                       MongoSplitStrategy splitStrategy) {
        this.clientProvider = clientProvider;
        this.deserializer = deserializer;
        this.splitStrategy = splitStrategy;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, FileSplit> createReader(SourceReaderContext sourceReaderContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<FileSplit, List<FileSplit>> createEnumerator(SplitEnumeratorContext<FileSplit> splitEnumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SplitEnumerator<FileSplit, List<FileSplit>> restoreEnumerator(SplitEnumeratorContext<FileSplit> splitEnumeratorContext, List<FileSplit> fileSplits) throws Exception {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<FileSplit> getSplitSerializer() {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<List<FileSplit>> getEnumeratorCheckpointSerializer() {
        return null;
    }

//    @Override
//    public SourceReader<T, MongoSplit> createReader(SourceReaderContext readerContext) throws Exception {
//        return new MongoReader<>(readerContext, clientProvider, deserializer);
//    }
//
//    @Override
//    public SplitEnumerator<MongoSplit, List<MongoSplit>> createEnumerator(
//            SplitEnumeratorContext<MongoSplit> enumContext) throws Exception {
//        return new MongoSplitEnumerator(enumContext, clientProvider, splitStrategy);
//    }
//
//    @Override
//    public SplitEnumerator<MongoSplit, List<MongoSplit>> restoreEnumerator(
//            SplitEnumeratorContext<MongoSplit> enumContext,
//            List<MongoSplit> checkpointedSplits) throws Exception {
//        return new MongoSplitEnumerator(enumContext, clientProvider, splitStrategy, checkpointedSplits);
//    }
//
//    @Override
//    public SimpleVersionedSerializer<MongoSplit> getSplitSerializer() {
//        return MongoSplitSerializer.INSTANCE;
//    }
//
//    @Override
//    public SimpleVersionedSerializer<List<MongoSplit>> getEnumeratorCheckpointSerializer() {
//        return ListMongoSplitSerializer.INSTANCE;
//    }
}
