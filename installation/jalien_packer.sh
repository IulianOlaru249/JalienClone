#!/bin/bash

cd `dirname $0`

# prepare Tomcat and compile the project
./compile.sh || exit 1

JL="./lib"

# the minimal set of JARs to start Tomcat and load the servler
for jar in alien.jar $JL/{FarmMonitor.jar,apmon.jar,bcpkix-jdk15on-152.jar,bcprov-jdk15on-152.jar,catalina.jar,javax.json-api-1.0.jar,jline-2.12.1.jar,jopt-simple-4.8.jar,json-simple-1.1.1.jar,servlet-api.jar,tomcat*.jar,lazyj.jar}; do
    jar -xf $jar
done

for a in javax/servlet/resources/{*.dtd,*.xsd}; do
    echo -n > $a
done

# package everything in a single JAR
jar -cfe jobagent.jar \
    alien.site.JobAgent alien/site/JobAgent.class \
    alien javax org lazyj apmon lia

# further compression and remove debugging information
pack200 --repack -G -O jobagent.jar

# remove all intermediate folders
rm -rf alien javax org META-INF lazyj scr/lazyj jline joptsimple lia apmonc com
