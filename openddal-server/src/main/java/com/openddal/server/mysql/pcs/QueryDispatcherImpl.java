package com.openddal.server.mysql.pcs;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.openddal.server.core.QueryDispatcher;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.ServerSession;

/**
 * 
 * @author jorgie.li
 *
 */
public class QueryDispatcherImpl extends CacheLoader<Integer, QueryProcessor> implements QueryDispatcher {

    private final DefaultQueryProcessor defaultProcessor;
    private LoadingCache<Integer, QueryProcessor> processors;

    public QueryDispatcherImpl(ServerSession session) {
        this.defaultProcessor = new DefaultQueryProcessor(session);
        this.processors = CacheBuilder.newBuilder().build(this);
    }

    @Override
    public QueryProcessor load(Integer type) throws Exception {
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
        int type = ServerParse.parse(query);
        QueryProcessor queryProcessor = processors.getUnchecked(type);
        return queryProcessor;
    }

}
