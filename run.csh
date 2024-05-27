#!/bin/csh

set cmd = $1
if ($cmd != "start" && $cmd != "stop") then
    echo "run.csh start/stop"
    exit
endif

set mypath = `dirname $0`
/usr/local/anaconda3/bin/python $mypath/manager.py koa $cmd
