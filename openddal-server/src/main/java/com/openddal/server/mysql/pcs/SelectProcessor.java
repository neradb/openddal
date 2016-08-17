package com.openddal.server.mysql.pcs;

import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class SelectProcessor implements QueryProcessor {

    private DefaultQueryProcessor target;

    public SelectProcessor(DefaultQueryProcessor target) {
        this.target = target;
    }

    @Override
    public QueryResult process(String query) {
        return null;
        
    }




}
