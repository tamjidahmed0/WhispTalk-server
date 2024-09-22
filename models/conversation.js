import mongoose from "mongoose";



const conversationSchema = new mongoose.Schema({

    senderName:{
        type:String
    },
    receiverName:{
        type:String
    },
    Name:{
        type:String
    },
    isVerified:{
        type:Boolean,
        default:false
    },
    text:{
        type:String
    },
    members:{
        type:Array
    },
    conversationFor: {
        type:mongoose.Types.ObjectId
    },
    For:{
        type:mongoose.Types.ObjectId
    },
    // userId:{
    //     type:String
    // },
    date:{
        type:Date,
        default:Date.now
    }
})

const collection = mongoose.model('Conversation', conversationSchema)

export default collection