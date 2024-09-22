import mongoose from "mongoose";



const unReadMessageSchema = new mongoose.Schema({

    senderId :{
        type: mongoose.Types.ObjectId
    },
    receiverId :{
        type:mongoose.Types.ObjectId
    },
    count:{
        type : Number,
        default: 1
    },
    
})

const collection = mongoose.model('unReadMessage', unReadMessageSchema)

export default collection