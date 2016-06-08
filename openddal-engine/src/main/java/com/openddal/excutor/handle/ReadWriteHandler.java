package com.openddal.excutor.handle;

public interface ReadWriteHandler {

    void close();
    /**
     * cancel a currently running PreparedExecutor. This operation will cancel all
     * opened JDBC statements and close all opened JDBC connections
     */
    void cancel();
    
    String explain();

}
