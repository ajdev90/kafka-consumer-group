const {Kafka} = require('@confluentinc/kafka-javascript').KafkaJS;

const consumer = new Kafka().consumer({
    'bootstrap.servers': 'my-broker:9092',
    'group.id': 'new-consumer-5'
});

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topics: ["new_topic"] });
    consumer.seek({'topic': 'new_topic','partition': 0,'offset' : "34"});
    consumer.seek({'topic': 'new_topic','partition': 1,'offset' : "29"});
    consumer.seek({'topic': 'new_topic','partition': 2,'offset' : "32"});
    consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                topic,
                partition,
                headers: message.headers,
                offset: message.offset,
                key: message.key?.toString(),
                value: message.value.toString(),
            });
            
        }
    });
}

run().catch(e => console.error(`Error: ${e.message}`, e))
