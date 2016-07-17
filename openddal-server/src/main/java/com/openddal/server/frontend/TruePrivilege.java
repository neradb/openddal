package com.openddal.server.frontend;

/**
 * Created by snow_young on 16/7/17.
 */
public class TruePrivilege implements Privilege {
    @Override
    public void init() {
    }

    @Override
    public boolean hasPrivilege(String clientName, String clientPass, String salt) {
        return true;
    }

    @Override
    public void reload() {
    }

    @Override
    public String get(String name) {
        return null;
    }
}
