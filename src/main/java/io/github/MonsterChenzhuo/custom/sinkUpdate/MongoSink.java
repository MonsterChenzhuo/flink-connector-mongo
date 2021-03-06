package io.github.MonsterChenzhuo.custom.sinkUpdate;



import io.github.MonsterChenzhuo.config.SinkConfiguration;
import io.github.MonsterChenzhuo.config.SinkConfigurationFactory;
import io.github.MonsterChenzhuo.serde.DocumentSerializer;
import io.github.MonsterChenzhuo.internal.connection.MongoClientProvider;
import io.github.MonsterChenzhuo.internal.connection.MongoColloctionProviders;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * 用于MongoDB的Flink sink连接器。
 * MongoDB 4.2+的事务模式，以及MongoDB 3.0+的非事务模式。
 * 在事务模式下，所有的写都将被缓冲在内存中，并在每个任务管理器事务中提交给MongoDB，这确保了精确一次的语义。
 * 在非事务模式下，写入将定期刷新到MongoDB，这提供了至少一次的语义。
 **/
public class MongoSink<IN> implements Sink<IN, DocumentBulk, DocumentBulk, Void>, SinkFunction<String> {

    private final MongoClientProvider clientProvider;

    private DocumentSerializer<IN> serializer;

    private final SinkConfiguration configuration;
    private final String key;

    public MongoSink(String connectionString,
                     String database,
                     String collection,
                     DocumentSerializer<IN> serializer,
                     Properties properties,
                     String key) {
        this.configuration = SinkConfigurationFactory.fromProperties(properties);
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders
                        .getBuilder()
                        .connectionString(connectionString)
                        .database(database)
                        .collection(collection).build();
        this.key = key;
    }

    @Override
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(InitContext initContext, List<DocumentBulk> states)
            throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<>(clientProvider, serializer, configuration,key);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (configuration.isTransactional()) {
            return Optional.of(new MongoCommitter(clientProvider));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }
}
