package source.split;

import org.apache.flink.api.connector.source.SourceSplit;
import org.bson.BsonDocument;

import java.io.File;

/**
 * MongoSplit is composed a query and a start offset.
 **/
public class FileSplit implements SourceSplit {

    private final String splitId;

    private final File query;

    private long startOffset;

    public FileSplit(String splitId, File query) {
        this(splitId, query, 0);
    }

    public FileSplit(String splitId, File query, long startOffset) {
        this.splitId = splitId;
        this.query = query;
        this.startOffset = startOffset;
    }

    public File getQuery() {
        return query;
    }

    public long getStartOffset() {
        return startOffset;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
