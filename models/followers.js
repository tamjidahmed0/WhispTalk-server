import mongoose from "mongoose";

const followersSchema = new mongoose.Schema({
    id:{
        type:mongoose.Types.ObjectId,
        ref:'user'
    },
    followers:{
        type: mongoose.Types.ObjectId,
      
    }
})

const collection = new mongoose.model('follower', followersSchema)
export default collection