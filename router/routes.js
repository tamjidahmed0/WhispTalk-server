import { Router } from "express";
import authenticate from "../middleware/authenticate.js";
import tokenMiddleware from "../middleware/tokenMiddleware.js";
import otpMiddleware from "../middleware/otpMiddleware.js";
import disableMiddleware from "../middleware/disableMiddleware.js";
import securityAlert from "../middleware/security-alert-middleware.js";

import * as controller from "../controllers/appController.js";

import {
  friendlistController,
  friendReqController,
  recommendationController,
  updateController,
  notificationController,
  loginController,
  registerController,

  otpController,
  socialController,
  followersController,
  followingController,
  unfollowController,
  countConnectionController,
  veryController,
  searchController,
  messageReqController,
  connectionRequest,
  getConnectionRequest,
  validateUser,
  otpValidate,
  EditAbout,
  ChangePassword,
  Search,
  conversation,
  changeEmail
} from "../controllers/index.js";
import multer from "multer";
import { UploadImage } from "../middleware/index.js";

// const storage = multer.diskStorage({
//     destination: (req, file, cb) =>{

//         cb(null, 'uploads/')

//     },
//     filename:(req, file, cb) =>{
//         console.log(req.params.id, 'come from req')
//         const uniqueFileName = Date.now() + '-' + file.originalname;
//         cb(null, uniqueFileName);
//     }
// })

// const upload = multer({ storage:storage });

// authenticate, (req, res)=>{res.setHeader('Content-Type', 'application/json');},

const router = Router();

//post req
router.route("/register").post(registerController);
router.route("/otp").post(otpMiddleware, otpController);
router.route("/resendotp").post(controller.resendotp);
router.route("/login").post(loginController);

router.route("/test/:id").post(UploadImage, controller.test);

// router.route("/conversations/:userId").get(authenticate, conversationController);
router.route("/verifytoken").post(veryController);
router.route("/connectionrequest/:userId/:requestId").post(connectionRequest)

router.route('/validateUser/:userId').get(authenticate, validateUser)
router.route('/validateOtp/:token').get(otpValidate)

router.route("/message").post(controller.message);
router.route("/message/:userId/:receiverId").get( authenticate, controller.getConversationId);
router.route("/getinfo/:userId").get(controller.getInfo);
router.route("/getConnectionReq/:userId").get(getConnectionRequest);
router.route("/profile/:userId").get(authenticate, controller.profile);
router.route("/conversation/:userId").get( authenticate, conversation);
router.route("/search/:userId").get( authenticate, Search);
router.route("/messagereq/:userId").get(messageReqController);
router.route("/notification/:userId").get( authenticate, notificationController);
router.route("/update/:userId").post( authenticate, updateController);
router.route("/friendrequest").post(friendReqController);
router.route("/friendlist/:userId").get(friendlistController);
 
router.route("/username/:userId").put(controller.userName); 
router.route("/name/:userId").put(controller.Name);
router.route("/about/:userId").put(EditAbout);
router.route("/changePassword/:userId").put(ChangePassword);
router.route("/changeEmail/:userId").put(changeEmail);
router.route("/disable").post(controller.disable);
  

router.route("/recommendation/:userId").get(recommendationController);
router.route("/image").post(controller.image);
router.route("/testSchedule").post(controller.schedule);

// router.route('/admin').post( controller.adminDashboard)
// router.route('/logout').post(controller.logOut) 

export default router;
