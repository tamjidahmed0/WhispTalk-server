import redisClient from "../redis-client/redisClient.js";

const callRejectEvent = async ({ io, socket, IS_USER_CALLING, ONLINE_USERS_KEY }) => {

  const deleteIsUserCalling = async ({ requestedId, rejectedId }) => {
    const hdel = await redisClient.hdel(IS_USER_CALLING, rejectedId);
    await redisClient.hdel(IS_USER_CALLING, requestedId);
    return hdel;
  };

  const getSocketUser = async (userId) => {
    const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
    return _userId;
  };

  socket.on("call:rejected", async (data) => {
    console.log(data, "call rejected");

    const delUserCall = await deleteIsUserCalling({ rejectedId: data.callRejectedId, requestedId: data.callRequestId });

    const _onlineUser = await getSocketUser(data.callRequestId);

    if (delUserCall) {
      console.log(delUserCall, '_onlineuser')
      io.to(_onlineUser).emit("call:rejected", {
        msg: "Call rejected",
      });
    }
  });
};

export default callRejectEvent;
