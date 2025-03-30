const {Kafka} = require('@confluentinc/kafka-javascript').KafkaJS;

const consumer = new Kafka().consumer({
    'bootstrap.servers': 'clm-pun-wuyfmk:9092',
    'group.id': 'new-consumer-1'
});

const run = async () => {
    await consumer.connect();
    await consumer.subscribe({ topics: ["new_topic"] });
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
