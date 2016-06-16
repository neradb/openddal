#!/bin/bash

ulimit -s 20480
ulimit -c unlimited
export PATH=$PATH:/usr/sbin

PRG="$0"
PRGDIR=`dirname "$PRG"`
BASEDIR=`cd "$PRGDIR/.." >/dev/null; pwd`

STATUS_FILE=${PRGDIR}/status
PID_FILE=${PRGDIR}/PID

LOGDIR=${BASEDIR}/logs
APP_LISTEN_PORT=1080
JMX_PORT=8060
SERVER_API=osp
START_TIME=60
RESTFUL_PORT=-1
TARGET_VERSION=1.7

USAGE()
{
  echo "usage: $0 start|stop|restart|status|info [-p|--port port] [-j|--jmx-port port] [-l|--log-dir dir] [-s|--server-api name] [-t|--start-timeout time] [-r|--restful-port port] [-e|--environment environment] [additional jvm args, e.g. -Dosp.loglevel=111 -Xmx2048m]"
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
    -s|--server-api) SERVER_API="$2" ; shift 2 ;;
    -t|--start-timeout) START_TIME="$2" ; shift 2 ;;
    -r|--restful-port) RESTFUL_PORT="$2" ; shift 2 ;;
    -e|--environment) RUN_ENVIRONMENT="$2" ; shift 2 ;;
    *) break ;;
  esac
done

ADDITIONAL_OPTS=$*;

if [[ "$RUN_ENVIRONMENT" = "dev" ]]; then
  ENVIRONMENT_MEM="-Xms512m -Xmx512m -Xss256K -XX:MaxDirectMemorySize=1024m"
else
  ENVIRONMENT_MEM="-Xms4096m -Xmx4096m -XX:MaxDirectMemorySize=4096m"
fi

if [ -d /dev/shm/ ]; then
	GC_LOG_FILE=/dev/shm/gc-$SERVER_API-$APP_LISTEN_PORT.log
else
	GC_LOG_FILE=${LOGDIR}/gc-$SERVER_API-$APP_LISTEN_PORT.log
fi

JAVA_OPTS="-Dosp.logfile=$LOGDIR/$SERVER_API -XX:+PrintCommandLineFlags -XX:-OmitStackTraceInFastThrow -XX:-UseBiasedLocking -XX:-UseCounterDecay -XX:AutoBoxCacheMax=20000 -Djava.net.preferIPv4Stack=true -Dio.netty.recycler.maxCapacity.default=0 -Dio.netty.leakDetectionLevel=disabled"
MEM_OPTS="-server ${ENVIRONMENT_MEM} -XX:NewRatio=1 -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=75 -XX:+UseCMSInitiatingOccupancyOnly -XX:+ParallelRefProcEnabled -XX:+AlwaysPreTouch -XX:MaxTenuringThreshold=6 -XX:+ExplicitGCInvokesConcurrent"
GCLOG_OPTS="-Xloggc:${GC_LOG_FILE} -XX:+PrintGCApplicationStoppedTime -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCDateStamps -XX:+PrintGCDetails"
CRASH_OPTS="-XX:ErrorFile=${LOGDIR}/hs_err_%p.log -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${LOGDIR}/"
JMX_OPTS="-Dcom.sun.management.jmxremote.port=${JMX_PORT} -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=127.0.0.1"
OTHER_OPTS="-Dstart.check.outfile=${STATUS_FILE}"

JAVA_VERSION=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}')

#if [[ "$JAVA_VERSION" < "1.7" ]]; then
#    echo "Error: Unsupported the java version $JAVA_VERSION , please use the version $TARGET_VERSION and above."
#    exit -1;
#fi

if [[ "$JAVA_VERSION" < "1.8" ]]; then
  MEM_OPTS="$MEM_OPTS -XX:PermSize=256m -XX:MaxPermSize=512m -XX:ReservedCodeCacheSize=300M -Djava.security.egd=file:/dev/./urandom"
  JAVA_OPTS="$JAVA_OPTS -javaagent:${BASEDIR}/mercurylib/aspectjweaver-1.7.3.jar"
else         
  MEM_OPTS="$MEM_OPTS -XX:MetaspaceSize=256m -XX:MaxMetaspaceSize=512m -XX:ReservedCodeCacheSize=480M"
  JAVA_OPTS="$JAVA_OPTS -javaagent:${BASEDIR}/mercurylib/aspectjweaver-1.8.6.jar"
fi


BACKUP_GC_LOG()
{
 GCLOG_DIR=${LOGDIR}
 BACKUP_FILE="${GCLOG_DIR}/gc-${SERVER_API}-${APP_LISTEN_PORT}_$(date +'%Y%m%d_%H%M%S').log"
 

 if [ -f ${GC_LOG_FILE} ]; then
  echo "saving gc log ${GC_LOG_FILE} to ${BACKUP_FILE}"
  mv ${GC_LOG_FILE} ${BACKUP_FILE}
 fi
}

GET_PID_BY_ALL_PORT()
{
  if [ "${RESTFUL_PORT}" = "-1" ]; then
    echo `lsof -n -P -i :${APP_LISTEN_PORT},${JMX_PORT} | grep LISTEN | awk '{print $2}' | head -n 1`
  else  
    echo `lsof -n -P -i :${APP_LISTEN_PORT},${JMX_PORT},${RESTFUL_PORT} | grep LISTEN | awk '{print $2}' | head -n 1`
  fi
}


STOP()
{
  BACKUP_GC_LOG
  
  if [ -f $PID_FILE ] ; then
	PID=`cat $PID_FILE`
  else
    PID=$(GET_PID_BY_ALL_PORT)	
  fi
  if [ "$PID" != "" ]
	then
	if [ -d /proc/$PID ];then
		LISTEN_STATUS=`cat ${STATUS_FILE}`
		echo "$SERVER_API stopping, ${LISTEN_STATUS}."
		kill $PID
		sleep 3

		if [ x"$PID" != x ]; then
		  echo -e "$SERVER_API still running as process:$PID,killing...\c"
		fi
		
		while  [ -d /proc/$PID ]; do
		  kill $PID
		  sleep 1
		  echo -e ".\c"
		done
			
		echo -e "\n$SERVER_API stop successfully"
	else
		echo "$SERVER_API is not running."
	fi
  else
	echo "$SERVER_API is not running."
  fi
}


START()
{
  BACKUP_GC_LOG
  echo "" > ${STATUS_FILE}

  if [ -f $PID_FILE ] ; then
	PID=`cat $PID_FILE`
  fi
  if [ "$PID" != "" ]
	then
	if [ -d /proc/$PID ];then
	 echo "$SERVER_API is running, please stop it first!!"
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

  echo "VIP_CFGCENTER_ZK_CONNECTION: ${VIP_CFGCENTER_ZK_CONNECTION}"
  echo "VIP_CFGCENTER_PARTITION: ${VIP_CFGCENTER_PARTITION}"
  
  if [ "${RESTFUL_PORT}" = "-1" ]; then
	LISTEN_STATUS="port is ${APP_LISTEN_PORT}, JMX port is ${JMX_PORT}"
  else
	LISTEN_STATUS="port is ${APP_LISTEN_PORT}, JMX port is ${JMX_PORT}, Restful port is ${RESTFUL_PORT}"
  fi
  echo "$SERVER_API starting, ${LISTEN_STATUS}."
  
  nohup java  $JAVA_OPTS $MEM_OPTS $GCLOG_OPTS $JMX_OPTS $CRASH_OPTS $OTHER_OPTS $ADDITIONAL_OPTS  -jar ${BASEDIR}/osp-engine.jar -servicesdir ${BASEDIR}/servicesdir -thirddir ${BASEDIR}/thirddir -port $APP_LISTEN_PORT -restport $RESTFUL_PORT >>$LOGDIR/osp-$SERVER_API.out 2>&1 &
  PID=$!
  echo $PID > $PID_FILE
  
  sleep 3
  
  
  CHECK_STATUS=`cat ${STATUS_FILE}`
  starttime=0
  while  [ x"$CHECK_STATUS" == x ]; do
    if [[ "$starttime" -lt ${START_TIME} ]]; then
      sleep 1
      ((starttime++))
      echo -e ".\c"
      CHECK_STATUS=`cat ${STATUS_FILE}`
    else
      echo -e "\n$SERVER_API start maybe unsuccess, start checking not finished until reach the starting timeout! See ${LOGDIR}/osp-${SERVER_API}.out for more information."
      exit -1
    fi
  done
  
  if [ $CHECK_STATUS = "SUCCESS" ]; then
	echo -e "\n$SERVER_API start successfully, running as process:$PID."
	echo ${LISTEN_STATUS} > ${STATUS_FILE}
  fi
  
  if [ $CHECK_STATUS = "ERROR" ]; then
	kill $PID
	echo -e "\n$SERVER_API start failed ! See ${LOGDIR}/osp-${SERVER_API}.out for more information."		
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
	  LISTEN_STATUS=`cat ${STATUS_FILE}`
	  echo "$SERVER_API is running ,${LISTEN_STATUS}."
	  exit 0
	fi
  fi
  echo "$SERVER_API is not running."
}

INFO()
{
	java -jar ${BASEDIR}/osp-engine.jar -action info -jmxPort $JMX_PORT -jmxHost 127.0.0.1
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