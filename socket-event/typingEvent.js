import onlineSchema from "../models/online.js";
import redisClient from "../redis-client/redisClient.js";

const typingEvent = ({ io, socket, ONLINE_USERS_KEY }) => {
  socket.on("typing", async (data) => {
    const textLength = data.typingValue.length;

    async function isUserOnline(userId) {
      const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
      return _userId;
    }

    console.log(data)

    const sendToUser = async (isUserTyping) => {
      const _userId = await isUserOnline(data.receiverId);
      console.log(_userId, "userid");
      io.to(_userId).emit("typing", {
        isUserTyping: isUserTyping,
        typerId: data.senderId,
      });
    }; 
 
    
 
    if (textLength !== 0) {
      console.log("typing...");
      await sendToUser(true);
    } else { 
      console.log("not typing");
      await sendToUser(false);
    }

    // const onlineData = await onlineSchema.findOne({ id: data.receiverId });
    // // const find = userData.find(obj =>obj.id === data.receiverId)
    // console.log(data);

    // const count = data.msg.length;
    // if (onlineData) {
    //   io.to(onlineData.socketId).emit("typingmsg", { ...data, count });
    // }
  });
};
export default typingEvent;
