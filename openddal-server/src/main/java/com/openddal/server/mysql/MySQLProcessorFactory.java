package com.openddal.server.mysql;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.processor.ProcessorFactory;
import com.openddal.server.processor.ProtocolProcessor;

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
