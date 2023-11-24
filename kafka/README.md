# Learning Kafka



### [How does Kafka work in a nutshell?](https://kafka.apache.org/documentation/#intro_nutshell)

Kafka is a distributed system consisting of **servers** and **clients** that
communicate via a high-performance [TCP network protocol](https://kafka.apache.org/protocol.html).
It can be deployed on bare-metal hardware, virtual machines, and containers in on-premise as well as cloud
environments.

**Servers**: Kafka is run as a cluster of one or more servers that can span multiple datacenters
or cloud regions. Some of these servers form the storage layer, called the brokers. Other servers run
[Kafka Connect](https://kafka.apache.org/documentation/#connect) to continuously import and export
data as event streams to integrate Kafka with your existing systems such as relational databases as well as
other Kafka clusters. To let you implement mission-critical use cases, a Kafka cluster is highly scalable
and fault-tolerant: if any of its servers fails, the other servers will take over their work to ensure
continuous operations without any data loss.

**Clients**: They allow you to write distributed applications and microservices that read, write,
and process streams of events in parallel, at scale, and in a fault-tolerant manner even in the case of network
problems or machine failures. Kafka ships with some such clients included, which are augmented by
[dozens of clients](https://cwiki.apache.org/confluence/display/KAFKA/Clients) provided by the Kafka
community: clients are available for Java and Scala including the higher-level
[Kafka Streams](https://kafka.apache.org/documentation/streams/) library, for Go, Python, C/C++, and
many other programming languages as well as REST APIs.

### [Main Concepts and Terminology](https://kafka.apache.org/documentation/#intro_concepts_and_terms)

An **event** records the fact that "something happened"
 in the world or in your business. It is also called record or message 
in the documentation. When you read or write data to Kafka, you do this 
in the form of events. Conceptually, an event has a key, value, 
timestamp, and optional metadata headers. Here's an example event:

- Event key: "Alice"
- Event value: "Made a payment of $200 to Bob"
- Event timestamp: "Jun. 25, 2020 at 2:06 p.m."

**Producers** are those client applications that publish (write) events to Kafka, and **consumers**
 are those that subscribe to (read and process) these events. In Kafka, 
producers and consumers are fully decoupled and agnostic of each other, 
which is a key design element to achieve the high scalability that Kafka
 is known for. For example, producers never need to wait for consumers. 
Kafka provides various [guarantees](https://kafka.apache.org/documentation/#semantics) such as the ability to process events exactly-once.

Events are organized and durably stored in **topics**. 
Very simplified, a topic is similar to a folder in a filesystem, and the
 events are the files in that folder. An example topic name could be 
"payments". Topics in Kafka are always multi-producer and 
multi-subscriber: a topic can have zero, one, or many producers that 
write events to it, as well as zero, one, or many consumers that 
subscribe to these events. Events in a topic can be read as often as 
needed—unlike traditional messaging systems, events are not deleted 
after consumption. Instead, you define for how long Kafka should retain 
your events through a per-topic configuration setting, after which old 
events will be discarded. Kafka's performance is effectively constant 
with respect to data size, so storing data for a long time is perfectly 
fine.

Topics are **partitioned**, meaning a topic is spread  over a number of "buckets" located on different Kafka brokers. This distributed placement of your data is very important for scalability 
because it allows client applications to both read and write the data 
from/to many brokers at the same time. When a new event is published to a
 topic, it is actually appended to one of the topic's partitions. Events
 with the same event key (e.g., a customer or vehicle ID) are written to
 the same partition, and Kafka [guarantees](https://kafka.apache.org/documentation/#semantics) that any consumer of a given topic-partition will always read that 
partition's events in exactly the same order as they were written.

![https://kafka.apache.org/images/streams-and-tables-p1_p4.png](https://kafka.apache.org/images/streams-and-tables-p1_p4.png)

Figure: This example topic has four partitions P1–P4. Two different producer clients are publishing, independently from each other, new events to the topic by writing events over the network to the topic'spartitions. Events with the same key (denoted by their color in the figure) are written to the same partition. Note that both producers can write to the same partition if appropriate.

To make your data fault-tolerant and highly-available, every topic can be **replicated**,
 even across geo-regions or datacenters, so that there are always 
multiple brokers that have a copy of the data just in case things go 
wrong, you want to do maintenance on the brokers, and so on. A common 
production setting is a replication factor of 3, i.e., there will always
 be three copies of your data. This replication is performed at the 
level of topic-partitions.

This primer should be sufficient for an introduction. The [Design](https://kafka.apache.org/documentation/#design) section of the documentation explains Kafka's various concepts in full detail, if you are interested.

## Topics in Kafka

> Kafka Binary Files Location: /usr/local/kafka/bin
> 

```bash
# List Topics
./kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create Topics
./kafka-topics.sh --create --topic likes --bootstrap-server localhost:9092

# Get topic description
./kafka-topics.sh --describe --topic likes --bootstrap-server localhost:9092
```

## Testing

```bash
# Run in Producer
bin/kafka-console-producer.sh --topic quickstart-events --bootstrap-server localhost:9092
```

```bash
# Run in Consumer
bin/kafka-console-consumer.sh --topic quickstart-events --from-beginning --bootstrap-server localhost:9092
```

## Refrences
[Apache Kafka](https://kafka.apache.org/documentation/#introduction)

[kafka-python — kafka-python 2.0.2-dev documentation](https://kafka-python.readthedocs.io/en/master/index.html)
