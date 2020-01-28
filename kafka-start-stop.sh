if [[ -z "${KAFKA_HOME}" ]]; then
  echo "KAFKA_HOME is undefined"
  exit 1
else
  nohup sh "${KAFKA_HOME}"/bin/zookeeper-server-start.sh "${KAFKA_HOME}"/config/zookeeper.properties > /dev/null 2>&1 &
  sleep 2
  nohup sh "${KAFKA_HOME}"/bin/kafka-server-start.sh "${KAFKA_HOME}"/config/server.properties > /dev/null 2>&1 &
  sleep 2
fi

