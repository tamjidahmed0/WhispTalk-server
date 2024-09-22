import mongoose from "mongoose";



const profileSchema = new mongoose.Schema({
   Id:{
    type:mongoose.Types.ObjectId,
   
   },
   name:{
    type:String
   },
   isVerified:{
      type:Boolean,
      default:false
  },
   username:{
    type:String
   },
   profilePic:{
    type:String
   },
   about:{
      type:String
   },
   profileUpdated:{
      type:Date,
      default:Date.now
   },
   date:{
      type:Date,
      default:Date.now
  }
})



// profileSchema.pre('save' , async function (next){






// })



const collection = mongoose.model('Profile', profileSchema)

export default collection