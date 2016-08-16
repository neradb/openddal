package com.openddal.server.mysql.pcs;

import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.util.ErrorCode;

public final class BeginProcessor implements QueryProcessor {
    
    @Override
    public boolean acceptsQuery(String query) {
        return true;
    }
    
    @Override
    public QueryResult process(String query) throws ServerException {
        throw ServerException.get(ErrorCode.ER_UNKNOWN_COM_ERROR, "Unsupported statement");
    }


}
