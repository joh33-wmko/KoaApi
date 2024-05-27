#!/bin/sh

if [ "$1" != "start" ] && [ "$1" != "stop" ]
then
    echo "run.csh start/stop"
    exit
fi

MYPATH=`dirname $0`
/usr/local/anaconda3/bin/python $MYPATH/manager.py koa $1
