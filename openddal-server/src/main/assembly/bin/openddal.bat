echo off
title osp-proxy
cd %cd%

set APP_LISTEN_PORT=1080
set JMX_PORT=8060


set JAVA_OPTS=-Dosp.container.threadpool.size=64 -Dosp.logfile=..\logs\osp
set MEM_OPTS=-server -Xms512m -Xmx512m -Xss256K -XX:MaxDirectMemorySize=1024m -XX:NewRatio=1 -XX:MaxPermSize=128m -XX:+UseParNewGC -XX:+UseConcMarkSweepGC
set GCLOG_OPTS=-Xloggc:..\logs\gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGC
set JMX_OPTS=-Dcom.sun.management.jmxremote.port=%JMX_PORT% -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=127.0.0.1

echo on
java  %JAVA_OPTS% %MEM_OPTS% %GCLOG_OPTS% %JMX_OPTS% -jar ..\osp-engine.jar -servicesdir ..\servicesdir -thirddir ..\thirddir -port %APP_LISTEN_PORT%