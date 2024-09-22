import onlineSchema from "../models/online.js";
import profileSchema from "../models/profile.js";
import redisClient from "../redis-client/redisClient.js";

const incommingCallEvent = async ({ io, socket, ONLINE_USERS_KEY, IS_USER_CALLING }) => {
  socket.on("user:incomming", async (data) => {
    console.log(data, "1127");

    // const onlineData = await onlineSchema.findOne({ id: data.requestForCalling });

    async function isUserOnline(userId) {
      const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
      return _userId;
    }

    const isUserAnotherCall = async ({ userId, isCalling, requestForCalling }) => {
      await redisClient.hset(IS_USER_CALLING, userId, isCalling);
      await redisClient.hset(IS_USER_CALLING, requestForCalling, isCalling);
    };
 
    const getUserAnotherCall = async ({ requestForCalling }) => {
      // const _userId = await redisClient.hget(IS_USER_CALLING , userId)
      const _requestForCalling = await redisClient.hget(IS_USER_CALLING, requestForCalling);

      // console.log(_userId, '~userid')
      console.log(_requestForCalling, "_requestForCalling");

      if (_requestForCalling === "true") {
        return true;
      } else {
        return false;
      }
    };

    const alreadyInCall = async ({ userId }) => {
      const _userId = await redisClient.hget(IS_USER_CALLING, userId);

      if (_userId === "true") {
        return true;
      } else {
        return false;
      }
    };

    const userInAnotherCall = await getUserAnotherCall({ userId: data.id, requestForCalling: data.requestForCalling });

    // console.log(userInAnotherCall, 'userInAnotherCall')

    const checkCall = await alreadyInCall({ userId: data.id });

    if (checkCall) {
      // const profileData = await profileSchema.findOne({ Id: data.requestForCalling });
      const _onlineUser = await isUserOnline(data.id);
      io.to(_onlineUser).emit("alreadyincall", { msg: `You are already in a call` });

      console.log("you are already in call");
    } else {
      if (userInAnotherCall) {
        console.log("user another call");
        const _onlineUser = await isUserOnline(data.id);
        io.to(_onlineUser).emit("useranothercall", { msg: "User is busy with another person" });
      } else {
        await isUserAnotherCall({ userId: data.id, requestForCalling: data.requestForCalling, isCalling: true });

        const onlineUser = await isUserOnline(data.requestForCalling);
        const profileData = await profileSchema.findOne({ Id: data.id });
        console.log(onlineUser, 'onlineuser')
//peerOffer: data.peerOffer,
        if (onlineUser) {
          io.to(onlineUser).emit("incommingoffer", { userId: data.id, name: profileData.name, profile: profileData.profilePic,  requestForCallingId: data.requestForCalling });
        }
      }
    }

    // await isUserAnotherCall({userId:data.id , requestForCalling:data.requestForCalling, isCalling:true})
  });
};

export default incommingCallEvent;
