import mongoose from "mongoose";

const online = new mongoose.Schema({
    socketId: {
        type:String
    },
    id:{
        type:mongoose.Types.ObjectId
    }
})

const collection = mongoose.model('online', online)

export default collection