const { Client, HighLevelProducer, } = require('kafka-node');

const {
    host,
    topic,
    message
} = require('minimist')(process.argv.slice(2));

const questionAsync = async question => new Promise((resolve, reject) => {
    rl.question(question, answer => {
        rl.close();
        resolve(answer);
    });
});

console.log(`Creating client for host: ${host}`);
const client = new Client(host);

console.log(`Creating producer`);
const producer = new HighLevelProducer(client);

producer.on('error', error => {
    console.log("Event.error", error);
});

producer.on('ready', readyRes => {
    console.log("Producer is now ready", { readyRes });

    // see payload model at: https://www.npmjs.com/package/kafka-node#highlevelproducer
    producer.send([{
        topic,
        messages: message // can be an array                    
    }], sendRes => {
        console.log("Sent", { sendRes });

        console.log("Closing client connection");
        client.close(closeRes => {
            console.log("Closed", { closeRes });
        });
    });
});
    