
import jwt from "jsonwebtoken";
import userschema from "../models/user.js";
import userOtp from "../models/otp.js";
import bcrypt from "bcrypt";
import { otpTimeouts } from "../controllers/registerController.js";
import { validateSecureToken, createSecureToken  } from "../services/tokenService.js";

const otpValidate = async (req, res) => {
  
  const token = req.params.token;

  // console.log(User, 'checkuser')


    try {


     
      // const result = jwt.verify(token, process.env.JWT_SECRET);
      const result = validateSecureToken(token, process.env.JWT_SECRET).payload




      const { username } = result;

   
      const findUserOtp = await userOtp.findOne({ username });
      console.log(findUserOtp);

      if (findUserOtp !== null) {
        //  next()
           res.status(201).send({isValidate:true})
      }else{
        res.status(404).send({isValidate:false, msg:'OTP expired'})
      }

      // req.user = await userschema.findById(userID).select('-password')
    } catch (error) {
      res.status(401).send({ status: "invalid token" });
    }
 
};

export default otpValidate;
