Simple helper tool to test out Kafka Connect SMTs and print the result to your terminal.

Edit `connect-standalone.properties` to point `bootstrap.servers` to your Kafka/[Redpanda](https://redpanda.com/) broker, then edit `logging-sink-connector.properties` with any converters and transforms that you want to test.

Then simply run `./run.sh`

(You may need to install Maven and a JDK — on macOS, you can try `brew install maven openjdk`)