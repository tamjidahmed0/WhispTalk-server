// import {Kafka} from 'kafkajs'
// import fs from "fs";
// import path from "path";
// import * as dotenv from 'dotenv' 
// dotenv.config()

// const kafka = new Kafka({
//     brokers: ['localhost:9092'],
//     // ssl: {
//     //   ca: [fs.readFileSync(path.resolve("./ca.pem"), "utf-8")],
//     // },
//     // sasl:{
//     //   mechanism: 'plain',
//     //   username: process.env.SASL_USERNAME,
//     //   password: process.env.SASL_PASSWORD,
//     // }
// })



// let producer = null;

// export async function createProducer() {
//   if (producer) return producer;

//   const _producer = kafka.producer();
//   await _producer.connect();
//   producer = _producer;
//   return producer;
// }


// export async function produceMessage(message) {
//     const producer = await createProducer();
//     await producer.send({
//       messages: [{ key: `message-${Date.now()}`, value: JSON.stringify(message) }],
//       topic: "MESSAGES",
//     });
//     return true;
//   }



//   export async function startMessageConsumer() {
//     console.log("Consumer is running..");
//     const consumer = kafka.consumer({ groupId: "default" });
//     await consumer.connect();
//     await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });
  
//     await consumer.run({
//       autoCommit: true,
//       eachMessage: async ({ message, pause }) => {
//         if (!message.value) return;
//         console.log(`New Message Recv..`);
//         console.log(message.value.toLocaleString())

//         // try {
//         //   await prismaClient.message.create({
//         //     data: {
//         //       text: message.value?.toString(),
//         //     },
//         //   });
//         // } catch (err) {
//         //   console.log("Something is wrong");
//         //   pause();
//         //   setTimeout(() => {
//         //     consumer.resume([{ topic: "MESSAGES" }]);
//         //   }, 60 * 1000);
//         // }
//       },
//     });
//   }
  // export default kafka;