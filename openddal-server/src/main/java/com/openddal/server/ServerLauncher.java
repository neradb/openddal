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
package com.openddal.server;

import com.openddal.server.mysql.MySQLServer;

/**
 * @author jorgie.li
 *
 */
public class ServerLauncher {

    private static NettyServer runingServer;

    private static ServerArgs parseArgs(String[] args) {
        ServerArgs serverArgs = new ServerArgs();
        if (args != null) {
            for (int i = 0; i < args.length; i += 2) {
                String key = args[i];
                if (i + 1 >= args.length) {
                    usage("the option " + key + " miss value.");
                }
                String value = args[i+1]; 

                if ("-port".equals(key)) {
                    if (value.matches("(-1)|([0-9]*)")) {
                        serverArgs.port(Integer.parseInt(value));
                    } else {
                        usage("-port should be positive integer");
                    }
                } else if ("-ssl".equals(key)) {
                    serverArgs.ssl(Boolean.valueOf(value));
                } else if ("-socketTimeoutMills".equals(key)) {
                    if (value.matches("([0-9]*)")) {
                        serverArgs.socketTimeoutMills(Integer.parseInt(value));
                    } else {
                        usage("-socketTimeoutMills should be positive integer");
                    }
                } else if ("-shutdownTimeoutMills".equals(key)) { 
                    if (value.matches("([0-9]*)")) {
                        serverArgs.shutdownTimeoutMills(Integer.parseInt(value));
                    } else {
                        usage("-shutdownTimeoutMills should be positive integer");
                    }
                }else if ("-sendBuff".equals(key)) { 
                    if (value.matches("([0-9]*)")) {
                        serverArgs.sendBuff(Integer.parseInt(value));
                    } else {
                        usage("-sendBuff should be positive integer");
                    }
                }else if ("-recvBuff".equals(key)) { 
                    if (value.matches("([0-9]*)")) {
                        serverArgs.shutdownTimeoutMills(Integer.parseInt(value));
                    } else {
                        usage("-recvBuff should be positive integer");
                    }
                } else if ("-bossThreads".equals(key)) { 
                    if (value.matches("([0-9]*)")) {
                        serverArgs.bossThreads(Integer.parseInt(value));
                    } else {
                        usage("-bossThreads should be positive integer");
                    }
                } else if ("-maxThreads".equals(key)) { 
                    if (value.matches("([0-9]*)")) {
                        serverArgs.maxThreads(Integer.parseInt(value));
                    } else {
                        usage("-workerThreads should be positive integer");
                    }
                } else if ("-protocol".equals(key)) {
                    serverArgs.protocol(value);
                } else if ("-configFile".equals(key)) {
                    serverArgs.configFile(value);
                } else if ("--help".equals(key)) {
                    usage(null);
                } else {
                    System.out.println("[warn][the parameter(" + key + ") is not supported now.]");
                }
            }
        }
        return serverArgs;
    }


    private static void usage(String message) {
        System.out.println();
        if (message != null) {
            System.out.println("Error: " + message);
            System.out.println();
        }
        System.out.println("Usage: java -jar jarfile [options] [args...]");
        System.out.println();
        System.out.println("The options include:");
        System.out.println("\t" + "--help: some helpful informations.");
        System.out.println("\t" + "-port: Integer, the port of server, default port is 6100.");
        System.out.println("\t" + "-configFile: engine config file.");
        System.out.println("\t" + "-bossThreads: Integer, set the netty bossThreads size.");
        System.out.println("\t" + "-maxThreads: Integer, set the netty workerThreads size.");
        System.out.println("\t" + "-userThreads: Integer, set size of the user thread pool that handle client request.");
        System.out.println("\t" + "-socketTimeoutMills: Integer, set the socket timeout in milliseconds.");
        System.out.println("\t" + "-shutdownTimeoutMills: Integer, set thread pool shutdown socket timeout in milliseconds.");
        System.out.println("\t" + "-sendBuff: Integer, the tcp option sendBuff");
        System.out.println("\t" + "-recvBuff: Integer, the tcp option recvBuff");
        System.out.println();
        System.exit(0);
    }


    
    public static void launch(String[] args) {
        try {
            ServerArgs serverArgs = parseArgs(args);
            if(serverArgs.port == -1) {
                serverArgs.port = NettyServer.DEFAULT_LISTEN_PORT;
            }
            NettyServer server = new MySQLServer(serverArgs);
            server.init();
            server.listen();
            runingServer = server;
            server.waitForClose();
        } catch (Exception e) {
            e.printStackTrace(System.err);
            System.exit(-1);
        } finally {
            runingServer = null;
        }
    }
    
    public static NettyServer getRuningServer() {
        return runingServer;
    }
    
    
    public static void main(String[] args) {
        launch(args);
    }



}
