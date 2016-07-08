#!/bin/bash

PRG="$0"
PRGDIR=`dirname "$PRG"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`
PID_FILE=${PRGDIR}/.pid

CLASSPATH=$CLASSPATH:$BASEDIR/lib/*:$BASEDIR/lib/drivers/*:$BASEDIR/conf:
SERVICE_NAME=openddal
LOGDIR=${BASEDIR}/logs
APP_LISTEN_PORT=6100
JMX_PORT=8050
START_TIME=60
CONFIG_FILE=engine.xml
TARGET_VERSION=1.6

USAGE()
{
  echo "usage: $0 start|stop|restart|status|info [-p|--port port] [-j|--jmx-port port] [-l|--log-dir dir] [-t|--start-timeout time] [-f|--config-file path] [-e|--environment environment] [additional jvm args, e.g. -Dopenddal.loglevel=111 -Xmx2048m]"
}

if [ $# -lt 1 ]; then
  USAGE
  exit -1
fi


CMD="$1"
shift

while true; do
  case "$1" in
    -p|--port) APP_LISTEN_PORT="$2" ; shift 2;;
    -j|--jmx-port) JMX_PORT="$2" ; shift 2 ;;
    -l|--log-dir) LOGDIR="$2" ; shift 2 ;;
    -t|--start-timeout) START_TIME="$2" ; shift 2 ;;
    -f|--config-file) CONFIG_FILE="$2" ; shift 2 ;;
    -e|--environment) RUN_ENVIRONMENT="$2" ; shift 2 ;;
    *) break ;;
  esac
done

ADDITIONAL_OPTS=$*;

if [[ "$RUN_ENVIRONMENT" = "dev" ]]; then
  ENVIRONMENT_MEM="-Xms512m -Xmx512m -Xss256K -XX:MaxDirectMemorySize=1024m"
else
  ENVIRONMENT_MEM="-Xms2048m -Xmx2048m -XX:MaxDirectMemorySize=2048m"
fi

if [ -d /dev/shm/ ]; then
  GC_LOG_FILE=/dev/shm/gc-$SERVICE_NAME-$APP_LISTEN_PORT.log
else
  GC_LOG_FILE=${LOGDIR}/gc-$SERVICE_NAME-$APP_LISTEN_PORT.log
fi

JAVA_OPTS="-Dopenddal.logfile=$LOGDIR/$SERVICE_NAME -XX:+PrintCommandLineFlags -XX:-OmitStackTraceInFastThrow -XX:-UseBiasedLocking -XX:-UseCounterDecay -XX:AutoBoxCacheMax=20000 -Djava.net.preferIPv4Stack=true -Dio.netty.recycler.maxCapacity.default=0 -Dio.netty.leakDetectionLevel=disabled"
MEM_OPTS="-server ${ENVIRONMENT_MEM} -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch -XX:MaxTenuringThreshold=6 -XX:+ExplicitGCInvokesConcurrent"
GCLOG_OPTS="-Xloggc:${GC_LOG_FILE} -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCDateStamps -XX:+PrintGCDetails"
CRASH_OPTS="-XX:ErrorFile=${LOGDIR}/hs_err_%p.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGDIR}/"
JMX_OPTS="-Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=127.0.0.1"
OTHER_OPTS=""

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')

#if [[ "$JAVA_VERSION" < "1.6" ]]; then
#    echo "Error: Unsupported the java version $JAVA_VERSION , please use the version $TARGET_VERSION and above."
#    exit -1;
#fi

if [[ "$JAVA_VERSION" < "1.8" ]]; then
  MEM_OPTS="$MEM_OPTS -XX:PermSize=256m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=300M -Djava.security.egd=file:/dev/./urandom"
else         
  MEM_OPTS="$MEM_OPTS -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:ReservedCodeCacheSize=480M"
fi


GET_PID_BY_ALL_PORT()
{
  echo `lsof -n -P -i :${APP_LISTEN_PORT},${JMX_PORT} | grep LISTEN | awk '{print $2}' | head -n 1`
}


STOP()
{
  
  if [ -f $PID_FILE ]; then
    PID=`cat $PID_FILE`
  else
    PID=$(GET_PID_BY_ALL_PORT)	
  fi
  if [ "$PID" != "" ]; then
	if [ -d /proc/$PID ];then
		echo "$SERVICE_NAME stopping..."
		kill $PID
		sleep 1
		
		while  [ -d /proc/$PID ]; do
		  kill $PID
		  sleep 1
		  echo -e ".\c"
		done
			
		echo -e "$SERVICE_NAME stop successfully"
	else
		echo "$SERVICE_NAME is not running."
	fi
  else
	echo "$SERVICE_NAME is not running."
  fi
}


START()
{

  if [ -f $PID_FILE ] ; then
	PID=`cat $PID_FILE`
  fi
  if [ "$PID" != "" ]
	then
	if [ -d /proc/$PID ];then
	 echo "$SERVICE_NAME is running, please stop it first!!"
	 exit -1
	fi
  fi
  
  if [ ! -d $LOGDIR ]; then
    echo "Warning! The logdir: $LOGDIR not existed! Try to create the dir automatically."
    mkdir $LOGDIR
    if [ -d $LOGDIR ]; then
      echo "Create logdir: $LOGDIR successed!"
    else
      echo "Create logdir: $LOGDIR failed, please check it!"
      exit -1
    fi
  fi  
  
  LISTEN_STATUS="port is ${APP_LISTEN_PORT}, JMX port is ${JMX_PORT}."

  echo "$SERVICE_NAME starting, ${LISTEN_STATUS}."
  
  nohup java  $JAVA_OPTS $MEM_OPTS $GCLOG_OPTS $JMX_OPTS $CRASH_OPTS $OTHER_OPTS $ADDITIONAL_OPTS -classpath "\"$CLASSPATH\"" com.openddal.server.ServerLauncher -configFile "$CONFIG_FILE" -port $APP_LISTEN_PORT >>$LOGDIR/$SERVICE_NAME.out 2>&1 &
  PID=$!
  echo $PID > $PID_FILE
  
  sleep 1
  
  
  CHECK_PID=$(GET_PID_BY_ALL_PORT)
  starttime=0
  while  [ x"$CHECK_PID" == x ]; do
    if [[ "$starttime" -lt ${START_TIME} ]]; then
      sleep 1
      ((starttime++))
      echo -e ".\c"
      CHECK_PID=$(GET_PID_BY_ALL_PORT)
    else
      echo -e "\n$SERVICE_NAME start maybe unsuccess, start checking not finished until reach the starting timeout! See ${LOGDIR}/${SERVICE_NAME}.out for more information."
      exit -1SEDIR/conf
    fi
  done
  
  if [ "$CHECK_PID" = "$PID" ]; then
    echo -e "$SERVICE_NAME start successfully, running as process:$PID."
  else
    kill $PID
    echo -e "$SERVICE_NAME start failed ! See ${LOGDIR}/${SERVICE_NAME}.out for more information." 
  fi
}


STATUS()
{
  if [ -f $PID_FILE ] ; then
	PID=`cat $PID_FILE`
  fi
  if [ "$PID" != "" ]
	then
	if [ -d /proc/$PID ];then
	  echo "$SERVICE_NAME is running."
	  exit 0
	fi
  fi
  echo "$SERVICE_NAME is not running."
}

INFO()
{
	java -jar ${BASEDIR}/engine.jar -action info -jmxPort $JMX_PORT -jmxHost 127.0.0.1
	exit 0;
}

case "$CMD" in
  stop) STOP;;
  start) START;;
  restart) STOP;sleep 3;START;;
  status) STATUS;;
  info) INFO;;
  help) USAGE;;
  *) USAGE;;
esac
