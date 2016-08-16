package com.openddal.server.mysql.pcs;

import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class RollbackProcessor implements QueryProcessor {

    @Override
    public boolean acceptsQuery(String query) {
        return true;
    }

    @Override
    public QueryResult process(String query) {
        return null;
    }

}
