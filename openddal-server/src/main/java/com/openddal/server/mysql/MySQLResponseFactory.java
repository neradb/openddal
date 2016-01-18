package com.openddal.server.mysql;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.processor.Response;
import com.openddal.server.processor.ResponseFactory;

/**
 * 
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class MySQLResponseFactory implements ResponseFactory {

    public Response createResponse(ProtocolTransport trans) {
        return new MySQLResponse(trans);
    }

}
