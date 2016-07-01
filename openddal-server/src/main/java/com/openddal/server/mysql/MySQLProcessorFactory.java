package com.openddal.server.mysql;

import com.openddal.server.ProcessorFactory;
import com.openddal.server.ProtocolProcessor;
import com.openddal.server.ProtocolTransport;

public class MySQLProcessorFactory implements ProcessorFactory {
    
    private ProtocolProcessor protocolProcessor;
    
    public MySQLProcessorFactory() {
        protocolProcessor = new MySQLProtocolProcessor();
    }

    @Override
    public ProtocolProcessor getProcessor(ProtocolTransport trans) {
        return protocolProcessor;
    }

}
