package com.openddal.server.mysql.pcs;

import com.openddal.server.core.QueryDispatcher;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.ServerSession;
import com.openddal.server.mysql.parser.ServerParse;

public class QueryDispatcherImpl implements QueryDispatcher {
    
    private final ServerSession session;
    
    public QueryDispatcherImpl(ServerSession session) {
        super();
        this.session = session;
    }



    @Override
    public QueryProcessor dispatch(String query) {
        int rs = ServerParse.parse(query);
        switch (rs & 0xff) {
        case ServerParse.SET:
            break;
        case ServerParse.SHOW:
            break;
        case ServerParse.SELECT:
            break;
        case ServerParse.START:
            break;
        case ServerParse.BEGIN:
            break;
        case ServerParse.LOAD:
            break;
        case ServerParse.SAVEPOINT:
            break;
        case ServerParse.USE:
            break;
        case ServerParse.COMMIT:
            break;
        case ServerParse.ROLLBACK:
            break;
        case ServerParse.EXPLAIN:
            break;
        default:
        }
        return null;
    }

}
