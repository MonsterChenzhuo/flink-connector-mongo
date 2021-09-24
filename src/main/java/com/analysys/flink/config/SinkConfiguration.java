package com.analysys.flink.config;

import com.analysys.flink.sink.MongoSink;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * Configuration for {@link MongoSink}.
 */
@Getter
@Setter
public class SinkConfiguration implements Serializable {
    //是否是事务
    private boolean isTransactional;
    //在检查是点上刷新
    private boolean isFlushOnCheckpoint;
    //桶大小
    private long bulkFlushSize;
    //桶刷写数据库间隔
    private long bulkFlushInterval;
}
