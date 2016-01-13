/*
 * Copyright 2014-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an “AS IS” BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.openddal.server.processor;

import java.io.UnsupportedEncodingException;

import com.openddal.server.ProtocolTransport;
import com.openddal.server.packet.ErrorPacket;

import io.netty.buffer.ByteBuf;

public class ResponseFactory {
    private static ResponseFactory instance = new ResponseFactory();

    private ResponseFactory() {
    }

    public static ResponseFactory getInstance() {
        return instance;
    }

    public Response createResponse(ProtocolTransport trans) {
        return new ResponseImpl(trans);
    }
    
    
    private static class ResponseImpl implements Response {
        
        private String charset;
        
        private ProtocolTransport trans;

        private ResponseImpl(ProtocolTransport trans) {
            this.trans = trans;
        }

        
        @Override
        public void sendError(int sc) {
            
        }

        /* (non-Javadoc)
         * @see com.openddal.server.processor.Response#sendError(int, java.lang.String)
         */
        @Override
        public void sendError(int errno, String msg) {
            getOutputByteBuf().clear();
            ErrorPacket err = new ErrorPacket();
            err.packetId = 1;
            err.errno = errno;
            err.message = encodeString(msg, charset);
            err.write(trans.out);            
        }


        @Override
        public ByteBuf getOutputByteBuf() {
            return trans.out;
        }
        
        private final static byte[] encodeString(String src, String charset) {
            if (src == null) {
                return null;
            }
            if (charset == null) {
                return src.getBytes();
            }
            try {
                return src.getBytes(charset);
            } catch (UnsupportedEncodingException e) {
                return src.getBytes();
            }
        }
        
    }
}
