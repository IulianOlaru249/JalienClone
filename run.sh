#!/bin/bash

JARFILE_CMD=$(echo "$@" | awk '{for ( x = 1; x <= NF; x++ ) { if ($x == "-jar") {print $(x+1)} }}') #'
[[ ( -e "${JARFILE_CMD}" ) && ( -f "${JARFILE_CMD}" ) ]] && JARFILE="${JARFILE_CMD}"

if [[ -z "${JARFILE}" ]]; then
  if [[ -z "${JALIEN_HOME}" ]]; then
    ## find the location of jalien script
    SOURCE="${BASH_SOURCE[0]}"
    while [ -h "${SOURCE}" ]; do ## resolve $SOURCE until the file is no longer a symlink
      JALIEN_HOME="$( cd -P "$(dirname "${SOURCE}" )" && pwd )" ##"
      SOURCE="$(readlink "${SOURCE}")" ##"
      [[ "${SOURCE}" != /* ]] && SOURCE="${JALIEN_HOME}/${SOURCE}" ## if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
    done
    JALIEN_HOME="$(cd -P "$( dirname "${SOURCE}" )" && pwd)" ##"
    export JALIEN_HOME
  fi

  JAR_LIST_MAIN="";
  for mainjar in ${JALIEN_HOME}/*.jar; do JAR_LIST_MAIN="${JAR_LIST_MAIN}${mainjar}:" ; done;
  JAR_LIST_MAIN=$( echo "${JAR_LIST_MAIN}" | sed 's/.$//')

  JAR_LIST_LIB="";
  for libjar in ${JALIEN_HOME}/lib/*.jar; do JAR_LIST_LIB="${JAR_LIST_LIB}${libjar}:" ; done;
  JAR_LIST_LIB=$( echo "${JAR_LIST_LIB}" | sed 's/.$//')

  export CLASSPATH="${JAR_LIST_MAIN}:${JAR_LIST_LIB}"
fi

if [ -z "$JALIEN_MEM" ]; then
    export JALIEN_MEM="-Xms64m -Xmx512m"
fi

if [ -z "$JALIEN_GC" ]; then
    export JALIEN_GC=" -XX:+UseG1GC -XX:+DisableExplicitGC -XX:MaxTrivialSize=1K"
fi

if [ -z "$JALIEN_JVM_OPTIONS" ]; then
    export JALIEN_JVM_OPTIONS="-server -XX:+OptimizeStringConcat -XX:CompileThreshold=20000"
fi

JALIEN_OPTS_DEFAULT="$JALIEN_JVM_OPTIONS $JALIEN_MEM $JALIEN_GC -Duserid=$(id -u) -Dcom.sun.jndi.ldap.connect.pool=false"

JAVA_VERSION=`java -version 2>&1 | sed -e 's/.*version "\([[:digit:]]*\)\(.*\)/\1/; 1q'`

if [ -z "$JAVA_VERSION" ]; then
    export JDK_JAVA_OPTIONS="--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED"
elif [ "$JAVA_VERSION" -ge 11 ]; then
    JALIEN_OPTS_DEFAULT="$JALIEN_OPTS_DEFAULT --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.rmi/sun.rmi.transport=ALL-UNNAMED"
fi

if [ ! -z "$TMPDIR" ]; then
  JALIEN_OPTS_DEFAULT="$JALIEN_OPTS_DEFAULT -Djava.io.tmpdir=$TMPDIR"
elif [ ! -z "$TMP" ]; then
  JALIEN_OPTS_DEFAULT="$JALIEN_OPTS_DEFAULT -Djava.io.tmpdir=$TMP"
elif [ ! -z "$TEMP" ]; then
  JALIEN_OPTS_DEFAULT="$JALIEN_OPTS_DEFAULT -Djava.io.tmpdir=$TEMP"
else
  JALIEN_OPTS_DEFAULT="$JALIEN_OPTS_DEFAULT -Djava.io.tmpdir=/tmp"
fi

CMD="java ${JALIEN_OPTS_DEFAULT}"
eval "${CMD}" "$@"
