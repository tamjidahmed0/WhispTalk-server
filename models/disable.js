import mongoose from "mongoose";


const disableSchema = new mongoose.Schema({
    Id:{
        type:String
    },
    identifier:{
        type:String
    },
    email:{
        type:String
    },
 
    Title:{
        type:String
    },
    Text:{
        type:String
    }
})

const collection = mongoose.model('DisableId', disableSchema)

export default collection