package com.openddal.executor.cursor;

import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.value.Value;
import com.openddal.value.ValueLong;

/**
 * The cursor implementation for the range index.
 */
public class RangeCursor implements Cursor {

    private boolean beforeFirst;
    private long current;
    private Row currentRow;
    private final long start, end, step;

    public RangeCursor(long start, long end) {
        this(start, end, 1);
    }

    public RangeCursor(long start, long end, long step) {
        this.start = start;
        this.end = end;
        this.step = step;
        beforeFirst = true;
    }

    @Override
    public Row get() {
        return currentRow;
    }

    @Override
    public SearchRow getSearchRow() {
        return currentRow;
    }

    @Override
    public boolean next() {
        if (beforeFirst) {
            beforeFirst = false;
            current = start;
        } else {
            current += step;
        }
        currentRow = createRow();
        return step > 0 ? current <= end : current >= end;
    }

    @Override
    public boolean previous() {
        throw DbException.throwInternalError();
    }
    
    
    private Row createRow() {
        return new Row(new Value[]{ValueLong.get(current)}, Row.MEMORY_CALCULATE);
    }

}
