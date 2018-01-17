const { Client, HighLevelProducer, } = require('kafka-node');

const {
    host,
    topic,
    message,
    showMetadata = false
} = require('minimist')(process.argv.slice(2));

console.log(`Creating client for host: ${host}`);
const client = new Client(host);

console.log(`Creating producer`);
const producer = new HighLevelProducer(client);

producer.on('error', error => {
    console.log("Event.error", error);
});

producer.on('ready', () => {

    if (showMetadata){
        client.loadMetadataForTopics([topic], (errr, result) => {
            console.log(`Loaded metadata for topic ${topic}`, result[1].metadata[topic]);
        });
    }

    console.log("Producer is now ready");

    // see payload model at: https://www.npmjs.com/package/kafka-node#highlevelproducer
    producer.send([{
        topic,
        messages: message // can be an array                    
    }], (error, data) => {
        console.log("Sent", { error, data });
        console.log("Closing client connection");
        client.close(() => console.log("Closed"));
    });
});
    