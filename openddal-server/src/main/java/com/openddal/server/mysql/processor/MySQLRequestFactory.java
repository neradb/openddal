package com.openddal.server.mysql.processor;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.processor.Request;
import com.openddal.server.processor.RequestFactory;

public class MySQLRequestFactory implements RequestFactory {


    public Request createRequest(ProtocolTransport trans) {
        return new MySQLRequest(trans);
    }

}
