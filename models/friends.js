import mongoose from "mongoose";



const friendsSchema = new mongoose.Schema({

    userId:{
        type:mongoose.Types.ObjectId
    },
    friendId:{
        type: mongoose.Types.ObjectId
    },
    Name:{
        type:String
    },
    isVerified:{
        type:Boolean,
        default:false
    },
    
    // userId:{
    //     type:String
    // },
    date:{
        type:Date,
        default:Date.now
    }
})

const collection = mongoose.model('Friend', friendsSchema)

export default collection