package com.openddal.server.mysql.pcs;

import java.util.Map;

import com.openddal.server.core.QueryDispatcher;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.ServerSession;
import com.openddal.util.New;

/**
 * 
 * @author jorgie.li
 *
 */
public class QueryDispatcherImpl implements QueryDispatcher {

    private final DefaultQueryProcessor defaultProcessor;
    private final Map<Integer, QueryProcessor> cacheProcessor;

    public QueryDispatcherImpl(ServerSession session) {
        this.defaultProcessor = new DefaultQueryProcessor(session);
        this.cacheProcessor = New.hashMap();
    }

    public QueryProcessor getQueryProcessor(Integer type) {
        QueryProcessor processor;
        switch (type) {
        case ServerParse.SET:
            processor = new SetProcessor(defaultProcessor);
            break;
        case ServerParse.SHOW:
            processor = new ShowProcessor(defaultProcessor);
            break;
        case ServerParse.SELECT:
            processor = new SelectProcessor(defaultProcessor);
            break;
        case ServerParse.START:
        case ServerParse.BEGIN:
        case ServerParse.SAVEPOINT:
        case ServerParse.COMMIT:
        case ServerParse.ROLLBACK:
            processor = new TransactionProcessor(defaultProcessor);
            break;
        case ServerParse.KILL:
        case ServerParse.KILL_QUERY:
            processor = new KillProcessor(defaultProcessor);
            break;
        case ServerParse.USE:
            processor = new UseProcessor(defaultProcessor);
            break;
        default:
            processor = defaultProcessor;

        }
        return processor;

    }

    @Override
    public QueryProcessor dispatch(String query) {
        int rs = ServerParse.parse(query);
        int type = rs & 0xff;
        QueryProcessor processor = cacheProcessor.get(type);
        if (processor == null) {
            processor = getQueryProcessor(type);
            cacheProcessor.put(type, processor);
        }
        return processor;
    }

}
