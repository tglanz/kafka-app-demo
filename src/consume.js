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
const consumer = new ConsumerGroup({
    host,
    groupId,
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'latest',
    autoCommit: false
}, topic);

try {
    console.log("Registering error event");
    consumer.on("error", error => {
        console.log("Event.error: ", error);
    })

    console.log("Registering message event");
    consumer.on('message', message => {
        console.log("Event.message", message);

        setTimeout(() => {
            consumer.commit((error, data) => {
                console.log("Committed", { data, error });
            });
        }, 0);
    })
} catch (error){
    console.error("Unknown error", error);
}