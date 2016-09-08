package com.openddal.executor.cursor;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.openddal.command.expression.Expression;
import com.openddal.engine.Session;
import com.openddal.message.DbException;
import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.value.DataType;
import com.openddal.value.Value;

public class ResultCursor implements Cursor {

    private Row current;
    private final ResultSet rs;
    private final Expression[] cols;

    public ResultCursor(Session session, ResultSet rs) {
        this.rs = rs;
        this.cols = Expression.getExpressionColumns(session, rs);
    }

    @Override
    public Row get() {
        if (current == null) {
            current = createRow();
            for (int i = 0; i < current.getColumnCount(); i++) {
                Value v = DataType.readValue(rs, i + 1, cols[i].getType());
                current.setValue(i, v);
            }
        }
        return current;
    }

    @Override
    public SearchRow getSearchRow() {
        return get();
    }

    @Override
    public boolean next() {
        try {
            boolean result = rs.next();
            if (!result) {
                rs.close();
                current = null;
                return false;
            }
        } catch (SQLException e) {
            throw DbException.convert(e);
        }
        current = null;
        return true;
    }

    @Override
    public boolean previous() {
        return false;
    }

    public Expression[] getExpressionColumns() {
        return cols;
    }

    private Row createRow() {
        return new Row(new Value[cols.length], Row.MEMORY_CALCULATE);
    }

}
