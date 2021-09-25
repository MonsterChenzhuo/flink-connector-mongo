package io.github.MonsterChenzhuo.source.split;

import java.util.List;

/**
 * @author chenzhuoyu
 * @date 2021/9/19 23:22
 * MongoSplitStrategy defines how to partition a Mongo data set into {@link MongoSplit}s.
 **/
public interface MongoSplitStrategy {

    List<MongoSplit> split();

}
