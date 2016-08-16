package com.openddal.server.core;

import com.openddal.server.ServerException;

/**
 * @author jorgie.li
 */
public interface QueryProcessor {
    
    boolean acceptsQuery(String query); 

    QueryResult process(String query) throws ServerException;
}
