import jwt from "jsonwebtoken";
import userschema from "../models/user.js";
import { validateSecureToken } from "../services/tokenService.js";

const authenticate = async (req, res, next) => {
  let token;

  const { authorization, userid } = req.headers;
  const paramsId = req.params.userId;

//   if((paramsId && userid) === 'undefined'){
// console.log('it is undefiend')
//   }else{
//     console.log('not undefiend')
//   }

// console.log(paramsId, 'paramsid')


  if (authorization) {
    try {
     
      if((paramsId && userid) !== 'undefined'){
        const User = await userschema.findById(paramsId);

        const checkuser = User !== null;
  
        token = authorization.split(" ")[1];
  
        const result2 =  validateSecureToken(token, process.env.JWT_SECRET).payload
  
  
  
        // const result = jwt.verify(token, process.env.JWT_SECRET);
  
        // console.log(result.id ===userid && paramsId === result.id, 'result')
  
  
  
      
          if (result2.id === userid && paramsId === result2.id && checkuser) {
          
            next();
          } else {
            res.status(401).send({ status: "Unauthorized!" });
          }
   
  
  
  
  
     
      }else{
        res.status(401).send({ status: "undefined!" });
      }
    

  

      // req.user = await userschema.findById(userID).select('-password')
    } catch (error) {
      console.log(error, 'error')
      res.status(401).send({ status: "invalid token" });
    }
  } else {
    res.status(401).send({ status: "please token provid" });
  }
};

export default authenticate;
