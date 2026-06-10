#!/bin/bash
set -e

if [ -f /opt/spark/conf/spark-defaults.conf.template ]; then
  mkdir -p /tmp/spark-conf
  envsubst < /opt/spark/conf/spark-defaults.conf.template > /tmp/spark-conf/spark-defaults.conf
  export SPARK_CONF_DIR=/tmp/spark-conf
fi

exec /opt/spark/bin/spark-class "$@"