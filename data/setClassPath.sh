#!/bin/bash

CP=$TP3/config/
CP=$CP:$( echo $TP3/lib/*.jar . | sed 's/ /:/g')
CP=$CP:$( echo $TP3/ext/*.jar . | sed 's/ /;/g')
CP=$CP:$(find -L $TP3/ext/ -name "*.jar" | tr '\n' ':')
export CP
export JAVA_OPTS="-Dlog4j.configuration=conf/log4j-server.properties -Xms32m -Xmx512m"
export JAVA_OPTS_DBG=$JAVA_OPTS" -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005"


