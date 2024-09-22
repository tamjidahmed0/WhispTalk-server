import jwt from "jsonwebtoken";
import userschema from "../models/user.js";
import userOtp from "../models/otp.js";
import bcrypt from "bcrypt";
import { otpTimeouts } from "../controllers/registerController.js";
import { validateSecureToken, createSecureToken  } from "../services/tokenService.js";

const authenticate = async (req, res, next) => {
  let token;

  const { authorization, userid } = req.headers;
  const paramsId = req.params.userId;

  // console.log(User, 'checkuser')

  if (authorization) {
    try {
      const { otp } = req.body;

      token = authorization.split(" ")[1];
      // const result = jwt.verify(token, process.env.JWT_SECRET);
      const result = validateSecureToken(token, process.env.JWT_SECRET).payload




      const { username } = result;

      console.log(username);
      const findUserOtp = await userOtp.findOne({ username });
      console.log(findUserOtp);

      if (findUserOtp !== null) {
        //  next()

        console.log(otp, "comne");
        console.log(otpTimeouts, "otptimeouts");
        const encOtp = await bcrypt.compare(otp, findUserOtp.otp);
        console.log(encOtp);

        if (encOtp) {
          const User = await userschema.findOne({ email: findUserOtp.email });
          console.log(User, "userschmea");

          if (User === null) {
            const userotp = await new userschema({
              name: findUserOtp.name,
              username: findUserOtp.username,
              email: findUserOtp.email,
              password: findUserOtp.password,
            }).save();

            const token = createSecureToken({
              username: userotp.username,
              id: userotp._id,
              exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24 * 365)
            },   process.env.JWT_SECRET)

            // const token = jwt.sign(
            //   {
            //     username: userotp.username,
            //     id: userotp._id,
            //   },
            //   process.env.JWT_SECRET,
            //   { expiresIn: "1y" }
            // );

            console.log(userotp, "userotp");

            if (userotp) {
              userOtp.findOneAndDelete({ email: userotp.email }, function (err, docs) {
                if (err) {
                  console.log(err, "error");
                } else {
                  clearTimeout(otpTimeouts[docs.email]); // Clear the timeout
                  delete otpTimeouts[docs.email]; // Remove the entry from the dictionary
                  console.log("Deleted User : ", docs);
                  // console.log(otpTimeouts, 'otptimeouts')
                  res.status(201).send({ id: userotp._id, token });
                  // next()
                }
              });
            }
          }
        }
      }

      // req.user = await userschema.findById(userID).select('-password')
    } catch (error) {
      res.status(401).send({ status: "invalid token" });
    }
  } else {
    res.status(401).send({ status: "please token provid" });
  }
};

export default authenticate;
