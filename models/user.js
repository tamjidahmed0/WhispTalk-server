import mongoose from "mongoose";
import bcrypt from "bcryptjs";
import profileSchema from "./profile.js";
import conversationSchema from "./conversation.js";
import requestSchema from './request.js'
import otp from './otp.js'


const schema = mongoose.Schema;

const UserSchema = new schema({
  profilePic: {
    type: String,
  },
  name: {
    type: String,
    require: true,
  },
  username: {
    type: String,
    require: true,
    // unique:true
  },
  email: {
    type: String,
  },
  password: {
    type: String,
    require: true,
  },
  refreshToken: {
    type: String,
  },
  date: {
    type: Date,
    default: Date.now,
  },
});

UserSchema.pre("save", async function (next) {
  const user = this;

  try {
    // Hash password only if it has changed
    // if (user.isModified("password")) {
    //   const salt = await bcrypt.genSalt(10);
    //   user.password = await bcrypt.hash(user.password, salt);
    // }


    const profile = await new profileSchema({
      Id: user._id,
      profilePic: `public/default.jpg`,
      name: user.name,
      username: user.username,
      about: ''
    }).save();

    if(profile){
      const otpFind = await otp.findOne({email:profile.email})


      if(otpFind !== null){

        // const otpDelete = await otp.findOneAndDelete({email:profile.email})


  


      }



    }



    next();
  } catch (error) {
    console.error("Error updating user or profile:", error);
    // Handle error appropriately, e.g., send an error response to the client
  }
});

UserSchema.post(["updateOne", "findOneAndUpdate", "findByIdAndUpdate", "updateMany"], async function (next) {
  const user = this;

  if (this.get("username")) {
    let userDoc = await mongoose.model("user").findOne(this._conditions);

    await profileSchema.findOneAndUpdate({ Id: userDoc._id }, { $set: { name: userDoc.name, username: userDoc.username } });

    const convFind = await conversationSchema.find({ conversationFor: userDoc._id });

    for (const item of convFind) {
      const conv1 = await conversationSchema.updateMany({ conversationFor: item.members[1] }, { receiverName: userDoc.name });
      const request = await requestSchema.updateMany({ conversationFor: item.members[1] }, { receiverName: userDoc.name });
    }
  }


});

const collection = mongoose.model("user", UserSchema);

export default collection;
