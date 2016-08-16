package com.openddal.server.core;

/**
 * @author jorgie.li
 */
public interface QueryProcessor {
    
    boolean acceptsQuery(String query); 

    QueryResult process(String query) throws QueryException;
}
