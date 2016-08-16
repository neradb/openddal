package com.openddal.server.core;

public interface QueryDispatcher {
    
    QueryProcessor dispatch(String query) throws QueryException;

}
