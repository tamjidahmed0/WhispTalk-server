import mongoose from "mongoose";



const notificationSchema = new mongoose.Schema({
    senderId: {
        type: String,
      },
      receiverId: {
        type: String,
      },
      text: {
        type: String,
      },
      isVerified:{
        type:Boolean,
        default:false
    },
      date:{
        type:Date,
        default:Date.now
    }
})

const notification = mongoose.model('Notification', notificationSchema)

export default notification