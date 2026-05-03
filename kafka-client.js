import { Kafka } from "kafkajs"


export const kafkaClient = new Kafka({
    clientId: 'tejas',
    brokers: ['localhost:9092']
})