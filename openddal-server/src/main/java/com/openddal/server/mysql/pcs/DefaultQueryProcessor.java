package com.openddal.server.mysql.pcs;

import com.openddal.command.Command;
import com.openddal.engine.Session;
import com.openddal.result.ResultInterface;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;
import com.openddal.server.core.ServerSession;
/**
 * @author jorgie.li
 */
public class DefaultQueryProcessor implements QueryProcessor {
    
    private final ServerSession session;

    public DefaultQueryProcessor(ServerSession session) {
        this.session = session;
    }

    public ServerSession getSession() {
        return session;
    }
    
    @Override
    public QueryResult process(String query) throws ServerException {
        QueryResult result;
        Session dbSession = session.getDbSession();
        Command command = null;
        try {
            synchronized (dbSession) {
                command = dbSession.prepareLocal(query);
                if (command.isQuery()) {
                    ResultInterface resultSet = command.executeQuery(0, false);
                    result = new QueryResult(resultSet);
                } else {
                    int updateCount = command.executeUpdate();
                    result = new QueryResult(updateCount);
                }
                return result;
            }
        } catch (Throwable e) {
            throw ServerException.convert(e);
        } finally {
            if (command != null) {
                command.close();
            }
        }
    }

}
