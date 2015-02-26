#!/bin/bash
filename=$1
java -Xmx240g -jar target/GraphConverters-0.0.1-SNAPSHOT-jar-with-dependencies.jar ${filename}
