// import kafka from "./kafka.js";
// import requestSchema from '../models/request.js'
// import messageSchema from '../models/message.js'

// import conversationSchema from '../models/conversation.js'
// import userschema from '../models/user.js'
// import profileSchema from '../models/profile.js' 


// export async function startMessageConsumer() {
//   console.log("Consumer is running..");
//   const consumer = kafka.consumer({ groupId: "default" });
//   await consumer.connect();
//   await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });

//   await consumer.run({
//     autoCommit: false,
    
//     eachMessage: async ({ message, topic, partition, pause }) => {
//       if (!message.value) return;
//       const consumer_msg = JSON.parse(message.value.toLocaleString())
//       console.log(`New Message Recv..`);
//       console.log(consumer_msg);
//       console.log(message.offset);
//       try {
//         // await consumer.commitOffsets([{  topic: "MESSAGES",partitions:[0],  offset: (parseInt(message.offset, 10) + 1).toString() }]);
        
       
// //  await consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset, 10)+1 ).toString() }]);
 
      
   
//         // throw new Error('hehe error')
    

   
//           const Message = new messageSchema({
//             receiverId : consumer_msg.receiverId,
//             senderId : consumer_msg.senderId,
//             textFor: consumer_msg.receiverId,
//             // receiverText: text,
//             text: consumer_msg.text,
//             type: consumer_msg.types,
//           });
//           const newMessage = new messageSchema({
//             senderId: consumer_msg.senderId,
//             receiverId: consumer_msg.receiverId,
//             // senderText: text,
//             textFor: consumer_msg.senderId,
//             text: consumer_msg.text,
//             type: consumer_msg.types,
//           });
//           const success = await newMessage.save();
//           const senderMessage = await Message.save();
    
//           if (success) {
          
    
    
    
//             const findUser = await userschema.find({
//               $or: [{ _id: senderMessage.senderId }, { _id: success.receiverId }],
//             });
    
//             if (findUser) {
//               const find = await conversationSchema.find({
//                 $and: [{ members: { $in: [senderMessage.senderId] } }, { members: { $in: [success.receiverId] } }],
//               });
    
//               if (Object.keys(find).length !== 0) {
//                 const originalText = success.text;
//                 const trimText = originalText.substring(0, 23);
    
//                 const filter = {
//                   $and: [{ conversationFor: senderMessage.senderId }, { For: success.receiverId }],
//                 };
//                 const filter2 = {
//                   $and: [{ conversationFor: success.receiverId }, { For: senderMessage.senderId }],
//                 };
    
          
//                 if (findUser[0]._id.toString() === consumer_msg.senderId) {
//                   console.log("line 271");
//                   await conversationSchema.findOneAndUpdate(filter, {
//                     $set: {
//                       text: trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`,
//                       date: new Date(),
//                     },
//                   });
    
//                   await conversationSchema.findOneAndUpdate(filter2, {
//                     $set: {
//                       text: `${trimText}`,
//                       date: new Date(),
//                     },
//                   });
    
//                   await requestSchema.findOneAndUpdate(filter2, {
//                     $set: {
//                       text: `${trimText}`,
//                       date: new Date(),
//                     },
//                   });
//                 } else {
//                   await conversationSchema.findOneAndUpdate(filter2, {
//                     $set: {
//                       text: `${trimText}.`,
//                       date: new Date(),
//                     },
//                   });
    
//                   await requestSchema.findOneAndUpdate(filter2, {
//                     $set: {
//                       text: `${trimText}.`,
//                       date: new Date(),
//                     },
//                   });
    
//                   await conversationSchema.findOneAndUpdate(filter, {
//                     $set: {
//                       text: trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`,
//                       date: new Date(),
//                     },
//                   });
//                 }
//               } else {
    
//                 const verified = await profileSchema.findOne({ Id: success.receiverId });
//                 const verified2 = await profileSchema.findOne({ Id: senderMessage.senderId });
    
//                 const originalText = success.text;
//                 const trimText = originalText.slice(0, 10);
    
//                 const conversation = new conversationSchema({
//                   senderName: "",
//                   receiverName: "",
//                   text: "",
//                   conversationFor: "",
//                   isVerified: verified.isVerified,
//                   For: "",
//                   members: [senderMessage.senderId, success.receiverId],
//                 });
    
//                 const newconversation = new requestSchema({
//                   senderName: "",
//                   receiverName: "",
//                   text: "",
//                   conversationFor: "",
//                   isVerified: verified2.isVerified,
//                   For: "",
//                   members: [success.receiverId, senderMessage.senderId],
//                 });
    
//                 if (findUser[0]._id.toString() === senderId) {
//                   console.log(success.text);
//                   conversation.senderName = findUser[0].name;
//                   conversation.receiverName = findUser[1].name;
//                   conversation.text = success.text;
    
//                   conversation.conversationFor = findUser[0]._id;
//                   conversation.For = findUser[1]._id;
    
//                   newconversation.senderName = findUser[1].name;
//                   newconversation.receiverName = findUser[0].name;
//                   newconversation.text = success.text;
    
//                   newconversation.conversationFor = findUser[1]._id;
//                   newconversation.For = findUser[0]._id;
//                 } else {
//                   console.log(success.text);
//                   conversation.senderName = findUser[1].name;
//                   conversation.receiverName = findUser[0].name;
//                   // conversation.text = success.text;
//                   conversation.text = trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`;
//                   conversation.conversationFor = findUser[1]._id;
//                   conversation.For = findUser[0]._id;
    
//                   newconversation.senderName = findUser[0].name;
//                   newconversation.receiverName = findUser[1].name;
//                   newconversation.text = success.text;
    
//                   newconversation.conversationFor = findUser[0]._id;
//                   newconversation.For = findUser[1]._id;
//                 }
    
//                 await conversation.save(); //msg that i send
//                 const requestmsg = await newconversation.save(); // request box msg
    
            
                
  
    
//               }
//             }
//           }
    
      
//           await consumer.commitOffsets([{ topic, partition, offset: (parseInt(message.offset, 10)+1 ).toString() }]);
 
//       } catch (err) {
//         console.log("Something is wrong", err);
//         pause();
//         // setTimeout(() => {
//         //   consumer.resume([{ topic: "MESSAGES" }]);
//         // },  1000);
//       }
//     },
//   });
// }