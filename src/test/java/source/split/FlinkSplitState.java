package source.split;

/**
 * The mutable version of mongo split.
 **/
public class FlinkSplitState extends FileSplit {

    private long currentOffset;

    public FlinkSplitState(FileSplit fileSplit) {
        super(FileSplit.splitId(), FileSplit.getQuery(), FileSplit.getStartOffset());
        this.currentOffset = FileSplit.getStartOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void increaseOffset(long n) {
        currentOffset += n;
    }
}
