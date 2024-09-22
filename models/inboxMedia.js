import mongoose from "mongoose";



const messageSchema = new mongoose.Schema({
    senderId: {
        type: String,
      },
      receiverId: {
        type: String,
      },
      link: {
        type: String,
      },
      type:{
        type:String
      },
      date:{
        type:Date,
        default:Date.now
    }
})

const collection = mongoose.model('inboxMedia', messageSchema)

export default collection