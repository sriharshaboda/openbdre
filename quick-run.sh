#!/bin/sh

java -jar auth-rest-api/target/dependency/jetty-runner.jar  --port 28851 \
--lib md-ui/target/md-ui-1.1-SNAPSHOT/WEB-INF/lib \
--lib auth-rest-api/target/auth-rest-api-1.1-SNAPSHOT/WEB-INF/lib \
 --classes auth-rest-api/target/classes \
 --classes md-ui/target/classes \
           auth-rest-api/context.xml \
           md-ui/context.xml
