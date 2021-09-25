package io.github.MonsterChenzhuo.sink;

import io.github.MonsterChenzhuo.config.MongoOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Tests for MongoSink insert mode.
 **/
public class MongoInsertSinkTest extends  MongoSinkTestBase {

    @Test
    public void testWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);

        // if these rows are not multiple times of rps, there would be the records remaining not flushed
        // after the last checkpoint
        long rps = 50;
        long rows = 1000L;

        Properties properties = new Properties();
        properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
        properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
        properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));

        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(new MongoSink<>(CONNECT_STRING, DATABASE_NAME, COLLECTION,
                        new StringDocumentSerializer(), properties));
        StreamGraph streamGraph = env.getStreamGraph(MongoTransactionalSinkTest.class.getName());

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
        }

        assertTrue(mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).countDocuments() > 0);
    }
}
