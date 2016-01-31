package com.openddal.server.pgsql;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.Request;
import com.openddal.server.RequestFactory;

public class PgSQLRequestFactory implements RequestFactory {


    public Request createRequest(ProtocolTransport trans) {
        return new PgSQLRequest(trans);
    }

}
