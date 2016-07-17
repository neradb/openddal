package com.openddal.server.frontend;

import com.openddal.engine.SysProperties;


/**
 * Created by snow_young on 16/7/17.
 */
public class PrivilegeFactory {
    private static PrivilegeFactory instance = new PrivilegeFactory();
    private static volatile Privilege privilege;

    private PrivilegeFactory(){}

    // config_path maybe change, so privilege will change
    public Privilege getConrrentPrivilege() {
        if (privilege == null) {
            synchronized(this){
                if(privilege == null) {
                    String path = SysProperties.USER_COFIG;
                    // TODO: ADD XML FORMAT FILE
                    if (path.endsWith("properties")) {
                        privilege = new PropPrivilege(path);
                    } else {
                        // default privilege implementation
                        privilege = new TruePrivilege();
                    }
                    privilege.init();
                }
            }
        }
        return privilege;
    }

    public static PrivilegeFactory getInstance(){
        return instance;
    }

}
