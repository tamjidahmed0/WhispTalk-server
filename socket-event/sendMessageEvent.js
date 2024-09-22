import messageSchema from "../models/message.js";
import userschema from "../models/user.js";
import onlineSchema from "../models/online.js";
import conversationSchema from "../models/conversation.js";
import profileSchema from "../models/profile.js";
import requestSchema from "../models/request.js";
import unReadMessageSchema from "../models/unReadMsg.js";
import redisClient from "../redis-client/redisClient.js";
import { Types } from "mongoose";

// import { produceMessage } from "../kafka/kafka.js";
// import { startMessageConsumer } from "../kafka/message-consumer.js";

const sendMessageEvent = async ({ io, socket, ONLINE_USERS_KEY }) => {
  // startMessageConsumer()

  socket.on("sendMessage", async (body) => {
    const { name, profile, senderId, receiverId, text, socketId, Dates, types } = body;
    const messageId = new Types.ObjectId();

    // await produceMessage(body)

    async function isUserOnline(userId) {
      const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
      return _userId;
    }

    const onlineUser = await isUserOnline(receiverId);
    const onlineUser_sender = await isUserOnline(senderId);
    console.log(onlineUser, "onlui be");

    io.to(onlineUser).emit("receivermessage", {
      // messageId: success._id,
      messageId,
      name: name,
      profile: profile,
      iSend: senderId,
      whoSend: receiverId,
      socketId: socketId,
      text: text,
      types: types,
      Date: Dates,
    });
 
    // socket.emit("sendermsg", {
    //   messageId,
    //   iSend: senderId,
    //   text: text,
    //   Date: Dates,
    //   receiverId: receiverId,
    //   messageStatus: "sent",
    // });


    io.to(onlineUser_sender).emit("sendermsg", {
      messageId,
      iSend: senderId,
      text: text,
      Date: Dates,
      receiverId: receiverId,
      messageStatus: "sent",
    });





//Here is chat list event for perticular situations

// const exists = await requestSchema.findOne({  }).lean() !== null;


// console.log(exists)



    io.to(onlineUser).emit("chat_list:receivermessage", {
      // messageId: success._id,
      name: name,
      profile: profile,
      iSend: senderId,
      whoSend: receiverId,
      socketId: socketId,
      text: text,
      types: types,
      Date: Dates,
    });



    socket.emit("chat_list:sendermsg", {
      iSend: senderId,
      name:name,
      profile: profile,
      text: text, 
      Date: Dates,
      receiverId: receiverId,
      messageStatus: "sent",
    });






    try {


      // Generate a new ObjectId for the first message



      const Message = new messageSchema({
        Id:messageId,
        receiverId,
        senderId,
        textFor: receiverId,
        // receiverText: text,
        text,
        type: types,
      });
      const newMessage = new messageSchema({
        Id:messageId,
        senderId,
        receiverId,
        // senderText: text,
        textFor: senderId,
        text,
        type: types,
      });
      const success = await newMessage.save();
      const senderMessage = await Message.save();

      //  await redisClient.publish('messages' , JSON.stringify({
      //   receiverId,
      //   senderId,
      //   textFor: receiverId,
      //   // receiverText: text,
      //   text,
      //   type: types,
      //  }))

      // console.log(senderMessage, 'successs')

      if (success) {
        const unreadmsgfind = await unReadMessageSchema.find({
          $and: [{ senderId: senderId }, { receiverId: receiverId }],
        });

        if (unreadmsgfind.length === 0) {
          const unread = await new unReadMessageSchema({
            senderId: senderId,
            receiverId: receiverId,
          }).save();

          // console.log(unread)
        } else {
          const updateUnread = await unReadMessageSchema.findOneAndUpdate(
            {
              $and: [{ senderId: senderId }, { receiverId: receiverId }],
            },
            {
              $inc: { count: 1 },
            },
            {
              new: true,
            }
          );
        }

        const findUser = await userschema.find({
          $or: [{ _id: senderMessage.senderId }, { _id: success.receiverId }],
        });

        if (findUser) {
          const find = await conversationSchema.find({
            $and: [{ members: { $in: [senderMessage.senderId] } }, { members: { $in: [success.receiverId] } }],
          });
 
          if (Object.keys(find).length !== 0) {
            const originalText = success.text;
            const trimText = originalText.substring(0, 23);

            const filter = {
              $and: [{ conversationFor: senderMessage.senderId }, { For: success.receiverId }],
            };
            const filter2 = {
              $and: [{ conversationFor: success.receiverId }, { For: senderMessage.senderId }],
            };

            const conversationExistingSender = await conversationSchema.findOne({ For: senderId });
            const conversationExistingReceiver = await conversationSchema.findOne({ For: receiverId });

            if (findUser[0]._id.toString() === senderId) {
              console.log("line 271");
              await conversationSchema.findOneAndUpdate(filter, {
                $set: {
                  text: trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`,
                  date: new Date(),
                },
              });

              await conversationSchema.findOneAndUpdate(filter2, {
                $set: {
                  text: `${trimText}`,
                  date: new Date(),
                },
              });

              await requestSchema.findOneAndUpdate(filter2, {
                $set: {
                  text: `${trimText}`,
                  date: new Date(),
                },
              });
            } else {
              await conversationSchema.findOneAndUpdate(filter2, {
                $set: {
                  text: `${trimText}.`,
                  date: new Date(),
                },
              });

              await requestSchema.findOneAndUpdate(filter2, {
                $set: {
                  text: `${trimText}.`,
                  date: new Date(),
                },
              });

              await conversationSchema.findOneAndUpdate(filter, {
                $set: {
                  text: trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`,
                  date: new Date(),
                },
              });
            }
          } else {
            const verified = await profileSchema.findOne({ Id: success.receiverId });
            const verified2 = await profileSchema.findOne({ Id: senderMessage.senderId });

            const originalText = success.text;
            const trimText = originalText.slice(0, 10);

            const conversation = new conversationSchema({
              senderName: "",
              receiverName: "",
              text: "",
              conversationFor: "",
              isVerified: verified.isVerified,
              For: "",
              members: [senderMessage.senderId, success.receiverId],
            });

            const newconversation = new requestSchema({
              senderName: "",
              receiverName: "",
              text: "",
              conversationFor: "",
              isVerified: verified2.isVerified,
              For: "",
              members: [success.receiverId, senderMessage.senderId],
            });

            if (findUser[0]._id.toString() === senderId) {
              console.log(success.text);
              conversation.senderName = findUser[0].name;
              conversation.receiverName = findUser[1].name;
              conversation.text = success.text;

              conversation.conversationFor = findUser[0]._id;
              conversation.For = findUser[1]._id;

              newconversation.senderName = findUser[1].name;
              newconversation.receiverName = findUser[0].name;
              newconversation.text = success.text;

              newconversation.conversationFor = findUser[1]._id;
              newconversation.For = findUser[0]._id;
            } else {
              console.log(success.text);
              conversation.senderName = findUser[1].name;
              conversation.receiverName = findUser[0].name;
              // conversation.text = success.text;
              conversation.text = trimText.length < 23 ? `You: ${trimText}.` : `You: ${trimText}...`;
              conversation.conversationFor = findUser[1]._id;
              conversation.For = findUser[0]._id;

              newconversation.senderName = findUser[0].name;
              newconversation.receiverName = findUser[1].name;
              newconversation.text = success.text;

              newconversation.conversationFor = findUser[0]._id;
              newconversation.For = findUser[1]._id;
            }

            await conversation.save(); //msg that i send
            const requestmsg = await newconversation.save(); // request box msg

            // const userfindonline = await onlineSchema.find({ id: receiverId });
            const countrequest = await requestSchema.countDocuments({
              conversationFor: receiverId,
            });

            if(requestmsg){

              io.to(onlineUser).emit('messageRequest', {
                count: countrequest,
                msg: `You have ${countrequest} message request`
              })

            } 


          }
        }
      }

     

  
    } catch (error) {
      // Handle error
      console.log(error);
    }
  });
};

export default sendMessageEvent;
