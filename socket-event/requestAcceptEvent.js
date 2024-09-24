import userschema from "../models/user.js";
import onlineSchema from "../models/online.js";
import conversationSchema from "../models/conversation.js";
import profileSchema from "../models/profile.js";
import requestSchema from "../models/request.js";
import notificationSchema from "../models/notification.js";
import redisClient from '../redis-client/redisClient.js'
import moment from "moment";


const requestAcceptEvent = async ({io, socket, ONLINE_USERS_KEY }) => {
  socket.on("messageAccept", async (data) => {
    console.log(data, 'request accept')
    try {
      const messageReqData = await requestSchema.findOne({
        conversationFor: data.userId,
        For: data.requestId,
      });
 
      const { name } = await userschema.findById(data.userId);

      const notification = await new notificationSchema({
        senderId: data.userId,
        receiverId: data.requestId,
        text: `${name} accepted your message request.`,
      }).save();

      console.log(notification, "notification");

      //  const messageReqData = await    requestSchema.aggregate([
      //         {
      //           $match: {
      //             $and: [
      //               { conversationFor: data.userId },
      //               { For: data.requestId }
      //             ]
      //           }
      //         },
      //         { $merge: { into: "Conversations" } }
      //       ]).exec()

      console.log(messageReqData);

      if (messageReqData) {
        // const verified = await profileSchema.findOne({Id:messageReqData.members[0]})
        const verified = await profileSchema.findOne({ Id: data.requestId });
      
        const transferToConversation = await new conversationSchema({
          senderName: messageReqData.senderName,
          receiverName: messageReqData.receiverName,
          text: messageReqData.text,
          conversationFor: messageReqData.conversationFor,
          For: messageReqData.For,
          isVerified: verified.isVerified,
          members: [messageReqData.members[0], messageReqData.members[1]],
        }).save();

        if (transferToConversation) {
          //   const deleteReqMessage = requestSchema.findOneAndDelete({
          //     conversationFor: data.userId,
          //     For: data.requestId
          //   })

          // console.log(deleteReqMessage, 'delete')

      

          const deleteReqMessage = requestSchema
            .findOneAndDelete({
              conversationFor: data.userId,
              For: data.requestId,
            })
            .then((deletedDoc) => {
              if (deletedDoc) {
                console.log("Document deleted:", deletedDoc);
              } else {
                console.log("No matching document found for deletion.");
              }
            });

          if (deleteReqMessage) {
            const countrequest = await requestSchema.countDocuments({
              conversationFor: data.userId,
            });

            async function isUserOnline(userId) {
              const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
              return _userId;
            }

            const send_to_receiver = await isUserOnline(data.requestId)
            const check_sender_online = await isUserOnline(data.userId)

            // const onlineData = await onlineSchema.findOne({ id: data.requestId });
             const profile = await profileSchema.findOne({Id:data.requestId})
             const profile_receiver = await profileSchema.findOne({Id:data.userId})
             
             io.to(send_to_receiver).emit('acceptNotification', {
              msg: notification.text,
              profile : profile_receiver.profilePic,
              Id: profile_receiver.Id,
              
             }) 


            const createdAt = moment(transferToConversation.date);
            const now = moment();
            const duration = moment.duration(now.diff(createdAt));
    
            const years = duration.years();
            const months = duration.months();
            const days = duration.days();
    
            const date = moment(transferToConversation.date);
            
            const getFormattedDate = (date) => {
              if (Math.abs(years)) {
                return `${date.format("MMM DD YYYY")}`;
              } else if (Math.abs(months)) {
                return `${date.format("MMM DD")}`;
              } else if (days) {
                return `${date.format("ddd")}`;
              } else {
                return date.format("hh:mm A");
              }
            };

            // io.to(check_online_user).emit("messageAccept", {
            //   // count: countrequest,
            //   id: profile.Id,
            //   profile: profile.profilePic,
            //   name: profile.name,
            //   text: transferToConversation.text
            // });
            io.to(check_sender_online).emit("messageAccept", {
              // count: countrequest,
              Id: profile.Id,
              profile: profile.profilePic, 
              name: profile.name,
              text: transferToConversation.text,
              date:getFormattedDate(date),
              verified:  transferToConversation.isVerified,
           
            });
 
 
          }
        } 
  
        console.log(transferToConversation, "saved");
      } else {
        console.log("No matching data found for the given conditions.");
      }
    } catch (error) {
      console.error("Error in accept socket event:", error);
    }
  });
};

export default requestAcceptEvent;
