package com.openddal.server.mysql.pcs;

import java.util.List;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.util.JdbcConstants;
import com.openddal.server.core.QueryProcessor;
import com.openddal.server.core.QueryResult;

public final class KillProcessor implements QueryProcessor {

    private DefaultQueryProcessor defaultQueryProcessor;
    
    public KillProcessor(DefaultQueryProcessor defaultQueryProcessor) {
        this.defaultQueryProcessor = defaultQueryProcessor;
    }

    @Override
    public QueryResult process(String query) {
        List<SQLStatement> stmts = SQLUtils.parseStatements(query, JdbcConstants.MYSQL);
        for (SQLStatement stmt : stmts) {
            
        }
        
        return null;
    }

}
