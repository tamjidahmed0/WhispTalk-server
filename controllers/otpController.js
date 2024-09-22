import userschema from "../models/user.js";
import profileSchema from "../models/profile.js";
import recommendationSchema from "../models/recommendation.js";
import userOtp from '../models/otp.js'

const otp = async (req, res) => {
  // const { otp } = req.body;
  // const getOtp = await userOtp.findOne({
    
  // })

  // const otpInt = parseInt(otp);
  // const domain = req.hostname;

console.log(req.body, 'otp')
res.status(201).send({ msg: "verified", otpPage: false });
};

export default otp;













































