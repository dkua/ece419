#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Usage: ./server.sh  <port>"
    exit 1
fi

if [ -z "$JAVA_HOME" ]; then
	JAVA_HOME=/usr
fi

${JAVA_HOME}/bin/java Server $1 

