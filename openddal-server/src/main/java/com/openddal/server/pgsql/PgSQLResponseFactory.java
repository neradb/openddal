package com.openddal.server.pgsql;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.Response;
import com.openddal.server.ResponseFactory;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class PgSQLResponseFactory implements ResponseFactory {

    public Response createResponse(ProtocolTransport trans) {
        return new PgSQLResponse(trans);
    }

}
