echo off
title openddal
cd %cd%

set APP_LISTEN_PORT=6100
set JMX_PORT=8050
set CONFIG_FILE=engine.xml


set JAVA_OPTS=-Dopenddal.logfile=..\logs\openddal -Dopenddal.stdout=true -Dopenddal.flowlog=true
set MEM_OPTS=-server -Xms512m -Xmx512m -Xss256K -XX:MaxDirectMemorySize=1024m -XX:NewRatio=1 -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -Xdebug -Xnoagent -Djava.compiler=NONE -Xrunjdwp:transport=dt_socket,address=5000,server=y,suspend=n
set GCLOG_OPTS=-Xloggc:..\logs\openddal_gc.log -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+PrintGC
set JMX_OPTS=-Dcom.sun.management.jmxremote.port=%JMX_PORT% -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=127.0.0.1
set CLASSPATH=%CLASSPATH%;..\lib\*;..\lib\drivers\*;..\conf;

echo on
java  %JAVA_OPTS% %MEM_OPTS% %GCLOG_OPTS% %JMX_OPTS% -classpath %CLASSPATH% com.openddal.server.ServerLauncher -configFile %CONFIG_FILE% -port %APP_LISTEN_PORT%