import mongoose from "mongoose";



const messageSchema = new mongoose.Schema({
  Id:{
    type:mongoose.Types.ObjectId
  },
    senderId: {
        type: mongoose.Types.ObjectId,
       
      },
      receiverId: {
        type: mongoose.Types.ObjectId,
       
      },
      text: {
        type: String,
      },
      textFor:{
        type:mongoose.Types.ObjectId
      },
      unsent:{
        type:Boolean,
        default:false
      },
      // senderText:{
      //   type:String
      // },
      // receiverText:{
      //   type:String
      // },
      type:{
        type: String
      },
      profileId:{
        type:mongoose.Schema.Types.ObjectId, 
        ref: 'Profile'
      },
 
      date:{
        type:Date,
        default:Date.now
    }
})

const collection = mongoose.model('Message', messageSchema)

export default collection