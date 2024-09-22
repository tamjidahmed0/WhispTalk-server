import mongoose from "mongoose";

const connectionRequests = new mongoose.Schema({
  requestSentId: {
    type: mongoose.Types.ObjectId,
  },
  requestReceiveId: {
    type: mongoose.Types.ObjectId,
  },
 

  date: {
    type: Date,
    default: Date.now,
  },
});

const collection = mongoose.model("ConnectionRequest", connectionRequests);

export default collection;
