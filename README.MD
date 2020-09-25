#Lombok in Eclipse(for refrence https://www.baeldung.com/lombok-ide)

If we're using Eclipse IDE, we need to get the Lombok jar first. The latest version is located on Maven Central. For our example, we're using lombok-1.18.4.jar.

Next, we can run the jar via java -jar command and an installer UI will open. This tries to automatically detect all available Eclipse installations, but it's also possible to specify the location manually.

Once we've selected the installations, then we press the Install/Update button:

If the installation is successful, we can exit the installer.

After installing the plugin, we need to restart the IDE and ensure that Lombok is correctly configured. We can check this in the About dialog:

#Start up the Zookeeper.
zookeeper-server-start.bat ..\..\config\zookeeper.properties

#Start up the Kafka Broker 0.
broker.id=0
listeners=PLAINTEXT://localhost:9092
auto.create.topics.enable=false
log.dirs=/tmp/kafka-logs
kafka-server-start.bat ..\..\config\server.properties

#Start up the Kafka Broker 1.
broker.id=1
listeners=PLAINTEXT://localhost:9093
auto.create.topics.enable=false
log.dirs=/tmp/kafka-logs-1
kafka-server-start.bat ..\..\config\server-1.properties

#Start up the Kafka Broker 2.
broker.id=2
listeners=PLAINTEXT://localhost:9094
auto.create.topics.enable=false
log.dirs=/tmp/kafka-logs-2
kafka-server-start.bat ..\..\config\server-2.properties


