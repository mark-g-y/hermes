# Hermes

## Introduction
Hermes is a distributed, scalable, and fault tolerant real-time messaging service. It allows programs to send messages to others using a publish/subscribe paradigm. Producers send messages to a specific channel. On the receiving side, consumers subscribe to a channel and a consumer group. Messages for a channel are delivered to all consumer groups which subscribe to it. The message is delivered to one consumer within each consumer group in a load-balanced fashion.
![alt tag](https://raw.githubusercontent.com/mark-g-y/hermes/master/documentation/img/architecture.png)

## Distributed Workers
Hermes uses ZooKeeper as a coordinator. Each worker registers itself with ZooKeeper on startup. On initialization, Hermes creates a fixed number of partitions. Workers are assigned to and removed from partitions based on scale and need. When a producer writes to a channel, hashing is used to determine its partition. The workers assigned to that partition are then used to handle that channel.

## Scaling
Each worker monitors its load status - currently, percentage of memory used is the metric used to determine load. Workers regularly updates this status in ZooKeeper. When new producers connect, they choose workers with lower loads. Each worker also ensures the load doesn’t exceed a preset threshold - if it does, detects the channel partition which is causing the heaviest load and allocates a new worker for it. It then disconnects half of its connections - the client will then connect to the newly created worker since it has the lowest load.

## Handling Failure
Hermes guarantees delivery using acknowledgements (acks). If a producer has received an ack, delivery is guaranteed. 

When the producer sends a message, it is replicated across a preset number of backup workers. There is one primary worker and N backups. The primary worker attempts to deliver the message while the backups store the message in memory. Upon successful delivery, the worker receives an ack. It will then send acks to its backup. If a backup does not receive an ack before the timeout, it will assume unsuccessful message delivery and resend the message. Each backup has a different timeout value - this way, timeouts on a single worker won’t cause all backups to send the message at the same time and cause duplicate messages.

### Successful Message Delivery
![alt tag](https://raw.githubusercontent.com/mark-g-y/hermes/master/documentation/img/success_send.png)

1. Message is sent by worker to consumer.
2. Ack is received by worker.
3. Worker sends ack to Backup #1.
4. Backup #1 sends ack to Backup #2.
5. Since all backups have received acks before their timeout, they won't attempt to resend the message.

### Unsuccessful Message Delivery
![alt tag](https://raw.githubusercontent.com/mark-g-y/hermes/master/documentation/img/unsuccess_send.png)

1. Message is received by worker, but it fails before the message can be delivered to the consumer.
2. Backup #1 does not receive an ack. It resends the message.
3. Consumer receives message and replies with an ack to Backup #1.
4. Backup #1 sends an ack to Backup #2.
5. Since Backup #2 has a higher timeout threshold, it will not attempt to resend until it receives this ack from Backup #1. Only if both the Primary worker and Backup #1 have failed will it attempt to resend the message.

That being said, there is still a chance of duplicate messages. If a worker sends a message successfully but fails before it can process the ack, the backup will detect a timeout and send the message again. This is common in other similar messaging services (e.g. Cherami and Kafka), and is not a major issue due to its unlikely probability of occurring.

One alternate design pattern that was explored was to not have backups and instead using end-to-end acks to guarantee delivery (producer sends to consumer through worker and waits for ack from consumer). Although this is simpler and reduces all possibilities of duplicate messages, it relies on resending from the client in case of worker failures. It also doesn’t allow storing message logs in a replicated fashion (this functionality isn’t currently implemented, but may be something to explore in the future).

## How to Use
Setup: You need to install and run ZooKeeper. After ZooKeeper is running, run the Main class in the “initializer” module and enter the ZooKeeper URL (or comma separated URLs if multiple nodes) as an argument.

Starting a Worker: Run the Main class in the “worker” module. Enter “help” as a command line argument to see the argument input format.

Starting a Producer: In your code, instantiate a new Producer class. Invoke the Producer.start() method before sending messages. Use the Producer.send() method to send messages. Documentation on constructor and method arguments can be found in the Javadocs (see “documentation” directory in the repository).

Starting a Consumer: In your code, instantiate a new Consumer class. You must invoke the Consumer.start() method before you can receive messages. Documentation on constructor and method arguments can be found in the Javadocs (see “documentation” directory in the repository).

## Potential Future Work Items
* Storing messages instead of “fire-and-forget” (useful for logging)
* Add libraries for non-Java programming languages (utilize Java “bridges”, e.g. Py4J for Python)
* Additional tests

## Acknowledgements
I studied some existing distributed messaging systems to help with my design of Hermes. I would like to acknowledge them here:

Jay Kreps, Neha Narkhede, and Jun Rao. "Kafka: a Distributed Messaging System for Log Processing." Web. 26 Dec. 2016. <http://www.longyu23.com/doc/Kafka.pdf>.

Xu Ning and Maxim Fateev. "Cherami: Uber Engineering's Durable and Scalable Task Queue in Go." Uber Engineering Blog. N.p., 6 Dec. 2016. Web. 26 Dec. 2016. <https://eng.uber.com/cherami/>.
