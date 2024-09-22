import userschema from "../models/user.js";
import profileSchema from "../models/profile.js";
import disableSchema from "../models/disable.js";
import bcrypt from "bcrypt";
import jwt from "jsonwebtoken";
import path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import maxmind from "maxmind";
import { Reader } from "@maxmind/geoip2-node";
import sendMail from "../mail/sendMail.js";
import useragent from "useragent";
// import redisClient from '../redis-client/redisClient.js'
import {createSecureToken} from '../services/tokenService.js'


//post request for login
const login = async (req, res) => {
  const domain = req.hostname;
  const forwarded = req.headers["x-forwarded-for"];
  const source = req.headers["user-agent"];
  const agent = useragent.parse(source);
  const ip = forwarded ? forwarded.split(",").shift() : req.ip;

  try {
    //collect data from body
    const { identifier, username, email, password } = req.body;

    if (Object.keys(identifier && password).length !== 0) {
      //find username and email
      const user = await userschema.findOne({
        $or: [{ username: identifier }, { email: identifier }],
      });

      console.log(user, "hh");

      if (user) {
        const encPass = await bcrypt.compare(password, user.password);
        if (encPass) {
          const disable = await disableSchema.findOne({
            $or: [{ identifier: identifier }, { email: identifier }],
          });

          if (disable) {
            res.status(400).send({ title: disable.Title, text: disable.Text, status: 400 });
          } else {
          

            const secureToken = createSecureToken({
              username: user.username,
              id: user._id,
              exp: Math.floor(Date.now() / 1000) + (60 * 60 * 24 * 365)


            }, process.env.JWT_SECRET)

        
            const profile = await profileSchema.findOne({ Id: user._id });

            const profilePic = profile ? profile.profilePic : `${domain}/public/default.jpg`;

            // if(cacheValue){
            //   res.status(201).send(cacheValue);
            // }else{
            //   await redisClient.set('loginDetails', JSON.stringify({ id: user._id, profile: profilePic, name: `${user.name}`, username: user.username, email: user.email, token: resendtoken, requestedIp : ip, status: 201 }))
            // }

            // await redisClient.set('loginDetails', JSON.stringify({ id: user._id, profile: profilePic, name: `${user.name}`, username: user.username, email: user.email, token: resendtoken, requestedIp : ip, status: 201 }))

            res.status(201).send({ id: user._id, profile: profilePic, name: `${user.name}`, username: user.username, email: user.email, token: secureToken, requestedIp: ip, status: 201 });

            // res.status(201).send({ id: user._id, profile: profilePic, name: `${user.name}`, username: user.username, email: user.email, token: resendtoken, requestedIp : ip, status: 201 });

            // res.status(201).cookie('token', resendtoken, {httpOnly: true, secure: false, sameSite:'none' }).cookie('c_user', user._id, {httpOnly: true, secure: false, sameSite:'none'})
            // .send({ id: user._id, profile: profilePic, name: `${user.name}`, username: user.username, email: user.email, token: resendtoken, status:201 })
          }
        } else {
          res.status(401).send({ msg: "username or password incorrect", status: 401 });
        }
      } else {
        res.status(401).send({ msg: "username or password incorrect", status: 401 });
      }
    } else {
      res.status(401).send({ msg: "Email and password should not be empty!", status: 401 });
    }
  } catch (error) {
    res.send(error);
    // await login()
  }
};

export default login;
