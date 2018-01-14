const { ConsumerGroup } = require('kafka-node');

const {
    host,
    groupId,
    topic
} = require('minimist')(process.argv.slice(2));

console.log("Received options:\n", {
    host,
    groupId,
    topic
});

console.log("Creating ConsumerGroup");
const consumerGroup = new ConsumerGroup({
    host,
    groupId,
    sessionTimeout: 2000,
    protocol: ['roundrobin'],
}, topic);

try {
    console.log("Registering error event");
    consumer.on("error", error => {
        console.log("Event.error: ", error);
    })

    console.log("Registering message event");
    consumerGroup.on('message', message => {
        console.log("Event.message", message);
    })

    console.log("Attempting to connect");
    consumerGroup.connect();

    
} catch (error){
    console.error("Unknown error", error);
}