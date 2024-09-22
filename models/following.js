import mongoose from "mongoose";

const followingSchema = new mongoose.Schema({
    id:{
        type:mongoose.Types.ObjectId,
        
    },
    following:{
        type: mongoose.Types.ObjectId,
        ref:'user'
    },
})

const collection = mongoose.model('following', followingSchema)

export default collection