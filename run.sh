#!/bin/sh

set -euxo pipefail

mvn compile exec:java -Dexec.args="connect-standalone.properties logging-sink-connector.properties"
