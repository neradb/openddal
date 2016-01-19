package com.openddal.server.mysql;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.Request;
import com.openddal.server.RequestFactory;

public class MySQLRequestFactory implements RequestFactory {


    public Request createRequest(ProtocolTransport trans) {
        return new MySQLRequest(trans);
    }

}
