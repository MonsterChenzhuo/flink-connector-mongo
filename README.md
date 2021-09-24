# flink-connector-mongo
flink连接mongo


```
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    // non-transactional sink with a flush strategy of 1000 documents or 10 seconds
    Properties properties = new Properties();
    properties.setProperty(MongoOptions.SINK_TRANSACTION_ENABLED, "false");
    properties.setProperty(MongoOptions.SINK_FLUSH_ON_CHECKPOINT, "false");
    properties.setProperty(MongoOptions.SINK_FLUSH_SIZE, String.valueOf(1_000L));
    properties.setProperty(MongoOptions.SINK_FLUSH_INTERVAL, String.valueOf(10_000L));

    env.addSource(...)
       .sinkTo(new MongoSink<>("mongodb://user:password@127.0.0.1:27017", "mydb", "mycollection",
                               new StringDocumentSerializer(), properties));

    env.execute();
```
