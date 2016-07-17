package com.openddal.server.frontend;


/**
 * Created by snow_young on 16/7/17.
 */
public interface Privilege {

    void init();

    boolean hasPrivilege(String clientName, String clientPass, String salt);

    void reload();

    String get(String name);
}