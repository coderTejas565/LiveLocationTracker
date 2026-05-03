import { kafkaClient } from "./kafka-client.js";

async function setup() {
    const admin = kafkaClient.admin();

    console.log(`Kafka Admin Connecting...`);
    await admin.connect();
    console.log(`kafka Admin Connecting Success...`);
    
    await admin.createTopics({
        topics: [{topic: `location-updates`, numPartitions: 2}]
    })

    await admin.disconnect();
    
}

setup()