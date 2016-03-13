#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: ./run.sh <server host> <server port>"
    exit 1
fi

if [ -z "$JAVA_HOME" ]; then
	JAVA_HOME=/usr
fi

${JAVA_HOME}/bin/java Mazewar $1 $2 

