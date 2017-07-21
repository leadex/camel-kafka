# Enhanced camel-kafka component

Based on camel-kafka:2.17.0.
Improved reliability of message consumption.

Added **startOffset** uri param. Kafka client start consuming from all partitions from this offset.
If startOffset not defined or equals -1, kafka client consider autoOffsetReset

## Example route:
```java
from("kafka:localhost:9092" +
        "?topic=test" +
        "&autoOffsetReset=earliest" +
        "&consumersCount=1" +
        "&groupId=group1" +
        "&autoCommitEnable=false" +
        "&startOffset=1337")
    .routeId("kafka-consumer")
    .log("\nMessage received from Kafka : ${body}" +
        "\n    on the topic ${header" + KafkaConstants.TOPIC + "}" +
        "\n    on the partition ${header" + KafkaConstants.PARTITION + "}" +
        "\n    with the offset ${header" + KafkaConstants.OFFSET + "}" +
        "\n    with the key ${header" + KafkaConstants.KEY + "}")
```

## Build
```bash
mvn clean package
```

## Install to fuse
0. Change version in pom.xml to your fuse version. Now is `2.17.0.redhat-630284`
1. Compile component.
2. Copy **target/camel-kafka-2.17.0.redhat-630284.jar** to **fuse-dir/system/org/apache/camel/camel-kafka/2.17.0.redhat-630284** folder.

If you have clear fuse. That's all.
If you have dirty fuse. You must remove camel-kafka bundle from cache.

3. Find camel-kakfa budle: `osgi:list | grep kafka`
4. Stop fuse. Remove budle directory from **fuse-dir/data/cache**
5. Start fuse.