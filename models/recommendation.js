import mongoose from "mongoose";



const recommendation = new mongoose.Schema({
   Id:{
    type:mongoose.Types.ObjectId,
   
   },
   name:{
    type:String
   },
   username:{
    type:String
   },
   profilePic:{
    type:String
   },
   isVerified:{
      type:Boolean,
      default:false
  },

   score:{
    type:Number,
    default: 1
   },

   date:{
      type:Date,
      default:Date.now
  }
})





const collection = mongoose.model('RecommendationScore', recommendation)

export default collection