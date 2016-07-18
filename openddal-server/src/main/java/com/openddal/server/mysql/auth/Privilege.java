package com.openddal.server.mysql.auth;

/**
 * Created by snow_young on 16/7/17.
 */
public interface Privilege {
    
    boolean userExists(String user);
    
    boolean schemaExists(String user, String schema);
    
    String password(String user);
    
    boolean checkPassword(String user, String password, String salt);
}