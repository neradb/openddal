package com.openddal.server.core;

import com.openddal.server.ServerException;

public interface QueryDispatcher {
    
    QueryProcessor dispatch(String query) throws ServerException;

}
