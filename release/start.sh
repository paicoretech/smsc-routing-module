#!/bin/bash

rmiIp=$(/usr/bin/curl ifconfig.me -w '\n')
rmiPort='9010'
jmxPort='9010'
java \
        -Dcom.sun.management.jmxremote \
        -Dcom.sun.management.jmxremote.port=$jmxPort \
        -Dcom.sun.management.jmxremote.local.only=false \
        -Dcom.sun.management.jmxremote.authenticate=false \
        -Dcom.sun.management.jmxremote.ssl=false -Djava.rmi.server.hostname=$rmiIp \
        -Dcom.sun.management.jmxremote.rmi.port=$rmiPort \
        -Xms256m -Xmx10g \
        -jar -Dspring.config.location=../conf/application.properties routing-VERSION.jar