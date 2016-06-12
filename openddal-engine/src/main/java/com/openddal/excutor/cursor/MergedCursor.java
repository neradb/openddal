package com.openddal.excutor.cursor;

import java.util.List;

import com.openddal.result.Row;
import com.openddal.result.SearchRow;
import com.openddal.util.New;

public class MergedCursor implements Cursor {

    private List<Cursor> cursors = New.arrayList(10);
    
    public void addCursor(Cursor cursor) {
        cursors.add(cursor);
    }
    
    @Override
    public Row get() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public SearchRow getSearchRow() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public boolean next() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean previous() {
        // TODO Auto-generated method stub
        return false;
    }

}
