package mongoflink.config;

import java.io.Serializable;
/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * 配置选项
 */

public class MongoOptions implements Serializable {

    public static final String SINK_TRANSACTION_ENABLED = "sink.transaction.enable";

    public static final String SINK_FLUSH_ON_CHECKPOINT = "sink.flush.on-checkpoint";
    //缓存数据大小
    public static final String SINK_FLUSH_SIZE = "sink.flush.size";
    //间隔
    public static final String SINK_FLUSH_INTERVAL = "sink.flush.interval";

}
