package com.openddal.server.mysql.pcs;

import com.openddal.server.core.QueryException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class CommitProcessor implements QueryProcessor {

    @Override
    public QueryResult process(String query) throws QueryException {
        return null;
    }

    @Override
    public boolean acceptsQuery(String query) {
        return true;
    }



}
