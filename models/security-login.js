import mongoose from "mongoose";

const securityLogin = new mongoose.Schema({
  Id: {
    type: mongoose.Types.ObjectId,
  },
  ip: {
    type: String,
  },
  device: {
    type: String,
  },
  app: {
    type: String,
  },
  location:{
    type:String
  },
  date: {
    type: Date,
    default: Date.now,
  },
});

const collection = mongoose.model("security_login", securityLogin);

export default collection;
