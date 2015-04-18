#!/bin/bash
#
# This script is used to start the server from a supplied config file
#

export CLIENT_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )/../.." && pwd )"
echo "** starting server from ${CLIENT_HOME} **"

echo poke home = $CLIENT_HOME
#exit

#cd ${CLIENT_HOME}

JAVA_MAIN='poke.client.snapchat.SnapchatClientCommand'

HOST="localhost"
PORT=6571
CLIENTID=12

# see http://java.sun.com/performance/reference/whitepapers/tuning.html
JAVA_TUNE='-Xms500m -Xmx1000m'


java ${JAVA_TUNE} -cp .:${CLIENT_HOME}/lib/'*':${CLIENT_HOME}/classes ${JAVA_MAIN} $HOST $PORT $CLIENTID
