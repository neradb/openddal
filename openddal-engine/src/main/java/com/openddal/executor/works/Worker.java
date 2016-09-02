package com.openddal.executor.works;

public interface Worker {
    
    /**
     * close the worker
     */
    void close();

    /**
     * cancel a currently running PreparedExecutor. This operation will cancel
     * all opened JDBC statements and close all opened JDBC connections
     */
    void cancel();

    /**
     * Get the handler with the execution explain.
     *
     * @return the execution explain
     */
    String explain();

}
