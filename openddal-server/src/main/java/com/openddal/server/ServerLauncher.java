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

/**
 * @author <a href="mailto:jorgie.mail@gmail.com">jorgie li</a>
 *
 */
public class ServerLauncher {


    private static ServerArgs parseArgs(String[] args) {
        ServerArgs serverArgs = new ServerArgs();
        if (args != null) {
            for (int i = 0; i < args.length; i += 2) {
                if ("-port".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-port miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.port(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-port should be positive integer");
                    }
                } else if ("-ssl".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-ssl miss value.");
                    }
                    serverArgs.ssl(Boolean.valueOf(args[i + 1]));
                } else if ("-socketTimeoutMills".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-socketTimeoutMills miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.socketTimeoutMills(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-socketTimeoutMills should be positive integer");
                    }
                } else if ("-shutdownTimeoutMills".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-shutdownTimeoutMills miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.shutdownTimeoutMills(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-shutdownTimeoutMills should be positive integer");
                    }
                }else if ("-sendBuff".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-sendBuff miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.sendBuff(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-sendBuff should be positive integer");
                    }
                }else if ("-recvBuff".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-recvBuff miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.shutdownTimeoutMills(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-recvBuff should be positive integer");
                    }
                } else if ("-bossThreads".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-bossThreads miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.bossThreads(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-bossThreads should be positive integer");
                    }
                } else if ("-workerThreads".equals(args[i])) {
                    if (i + 1 >= args.length) {
                        fail("-workerThreads miss value.");
                    }
                    if (args[i + 1].matches("(-1)|([0-9]*)")) {
                        serverArgs.workerThreads(Integer.parseInt(args[i + 1]));
                    } else {
                        fail("-workerThreads should be positive integer");
                    }
                } else if ("--help".equals(args[i])) {
                    fail(null);
                } else {
                    System.out.println("[warn][the parameter(" + args[i] + ") is not supported now.]");
                }
            }
        }
        return serverArgs;
    }


    private static void fail(String message) {
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
        System.out.println("\t" + "-ssl: True or false, if true, the server will use ssl protocol.");
        System.out.println("\t" + "-bossThreads: Integer, set the netty bossThreads size.");
        System.out.println("\t" + "-workerThreads: Integer, set the netty workerThreads size.");
        System.out.println("\t" + "-userThreads: Integer, set size of the user thread pool that handle client request.");
        System.out.println("\t" + "-socketTimeoutMills: Integer, set the socket timeout in milliseconds.");
        System.out.println("\t" + "-shutdownTimeoutMills: Integer, set thread pool shutdown socket timeout in milliseconds.");
        System.out.println("\t" + "-sendBuff: Integer, the tcp option sendBuff");
        System.out.println("\t" + "-recvBuff: Integer, the tcp option recvBuff");
        System.out.println();
        System.exit(0);
    }


    
    private static void launch(String[] args) {
        try {
            ServerArgs serverArgs = parseArgs(args);
            NettyServer server = new NettyServer(serverArgs);
            server.listen();
            server.waitForClose();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
    
    
    public static void main(String[] args) {
        launch(args);
    }



}
