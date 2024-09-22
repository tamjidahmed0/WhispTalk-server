import mongoose from "mongoose";


const videoCountSchema = new mongoose.Schema({
    videoId:{
        type: mongoose.Types.ObjectId
    },
    userId:{
        type:mongoose.Types.ObjectId
    },
    views:{
        type: Number,
        default: 0
    }
})

const collection = mongoose.model('video', videoCountSchema)
export default collection