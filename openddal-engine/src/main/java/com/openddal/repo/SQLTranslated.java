package com.openddal.repo;

import java.util.List;

import com.openddal.value.Value;

public class SQLTranslated {

    public String sql;

    public List<Value> params;
    
    public static SQLTranslated build() {
        SQLTranslated optional = new SQLTranslated();
        return optional;
    }
    
    public SQLTranslated sql(String sql) {
        this.sql = sql;
        return this;
    }
    
    
    public SQLTranslated sqlParams(List<Value> params) {
        this.params = params;
        return this;
    }
    
}
