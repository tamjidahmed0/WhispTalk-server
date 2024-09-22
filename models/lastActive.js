import mongoose from "mongoose";



const lastActiveSchema = new mongoose.Schema({
      Id: {
        type: mongoose.Types.ObjectId,
      },
      lastActive:{
        type:Date,
        default:Date.now
      },
     
})

const collection = mongoose.model('lastActive', lastActiveSchema)

export default collection