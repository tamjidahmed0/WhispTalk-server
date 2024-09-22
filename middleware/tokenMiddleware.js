import jwt from "jsonwebtoken";
import userschema from "../models/user.js";

const tokenMiddleware = async (req, res) => {
  let token;

  const { authorization } = req.headers;

  if (authorization) {
    try {
      token = authorization.split(" ")[1];
      const result = jwt.verify(token, process.env.JWT_SECRET);

      res;

      // req.user = await userschema.findById(userID).select('-password')
    } catch (error) {
      res.status(401).send({ status: "invalid token" });
    }
  } else {
    res.status(401).send({ status: "please token provid" });
  }
};

export default tokenMiddleware;
