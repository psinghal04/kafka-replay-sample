if [[ -z "${KAFKA_HOME}" ]]; then
  echo "set KAFKA_HOME first"
  exit 1
fi

PATH=$PATH:$KAFKA_HOME/bin

# See how we were called.
case "$1" in
  start)
        # Start daemon.
        echo "Starting Zookeeper";
        nohup $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon /$KAFKA_HOME/config/zookeeper.properties 2> /dev/null && \
        echo "Starting Kafka";
        nohup $KAFKA_HOME/bin/kafka-server-start.sh -daemon /$KAFKA_HOME/config/server.properties 2> /dev/null
        ;;
  stop)
        # Stop daemons.
        echo "Shutting down Zookeeper";
        pid=`ps ax | grep -i 'org.apache.zookeeper.server' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          kill -9 $pid
        else
          echo "Zookeeper was not Running"
        fi
        echo "Shutting down Kafka";
        pid=`ps ax | grep -i 'kafka.Kafka' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          kill -9 $pid
        else
          echo "Kafka was not Running"
        fi
        ;;
  restart)
        $0 stop
        sleep 2
        $0 start
        ;;
  status)
        pid=`ps ax | grep -i 'org.apache.zookeeper.server' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          echo "Zookeeper is Running as PID: $pid"
        else
          echo "Zookeeper is not Running"
        fi
        pid=`ps ax | grep -i 'kafka.Kafka' | grep -v grep | awk '{print $1}'`
        if [ -n "$pid" ]
          then
          echo "Kafka is Running as PID: $pid"
        else
          echo "Kafka is not Running"
        fi
        ;;
  *)
        echo "Usage: $0 {start|stop|restart|status}"
        exit 1
esac

exit 0