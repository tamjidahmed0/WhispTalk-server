import redisClient from "../redis-client/redisClient.js";


const callAcceptEvent = ({ io, socket, ONLINE_USERS_KEY }) => {
  socket.on("call:accepted", async (data) => {
    // const userFind = await onlineSchema.findOne({ id: data.userId });

    async function isUserOnline(userId) {
      const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
      return _userId;
    }

    const userSocket = await isUserOnline(data.requestForCalling);

    console.log(data, "accepted");

    io.to(data.socket).emit("call:accepted", { userId: data.userId, peerAnswer: data.peerAnswer });
  });
};

export default callAcceptEvent;
