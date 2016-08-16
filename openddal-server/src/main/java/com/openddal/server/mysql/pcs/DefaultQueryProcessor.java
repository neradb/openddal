package com.openddal.server.mysql.pcs;

import com.openddal.command.Command;
import com.openddal.engine.Session;
import com.openddal.result.ResultInterface;
import com.openddal.server.ServerException;
import com.openddal.server.core.QueryResult;
import com.openddal.server.core.ServerSession;
import com.openddal.server.core.SessionQueryProcessor;

public class DefaultQueryProcessor extends SessionQueryProcessor {

    public DefaultQueryProcessor(ServerSession session) {
        super(session);
    }

    @Override
    public boolean acceptsQuery(String query) {
        return true;
    }

    @Override
    public QueryResult process(String query) throws ServerException {
        QueryResult result;
        Session dbSession = session.getDatabaseSession();
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
