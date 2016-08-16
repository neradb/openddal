package com.openddal.server.mysql.pcs;

import com.openddal.server.core.QueryException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;


public final class StartProcessor implements QueryProcessor {

    @Override
    public boolean acceptsQuery(String query) {
        return false;
    }

    @Override
    public QueryResult process(String query) throws QueryException {
        return null;
    }

}
