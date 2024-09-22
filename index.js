import express  from "express"
import path from 'path'
import { fileURLToPath } from 'url';
import  {dirname}  from "path"
//import Db 
import connectDB from "./config/db.js"
//import router
import  router  from './router/routes.js'
//import dotenv
import * as dotenv from 'dotenv' 
dotenv.config()
//import session
import session from "express-session";
//import cors
import cors from 'cors'
//import bodyparser
import bodyParser from "body-parser"
//import cookie parser
import cookieParser from "cookie-parser"

import { Server } from "socket.io";
import http from "http";
import recommendation from './recommendation/recommendation.js'
import lastActiveSchema from './models/lastActive.js'

import messageSchema from "./models/message.js";
import userschema from "./models/user.js";
import onlineSchema from "./models/online.js";
import disableSchema from './models/disable.js'

import userOtp from './models/otp.js'
import jwt  from 'jsonwebtoken'
import redisClient from './redis-client/redisClient.js'
import fs from 'fs'

import sendMessageEvent from "./socket-event/sendMessageEvent.js";
import requestAcceptEvent from "./socket-event/requestAcceptEvent.js";
import incommingCallEvent from "./socket-event/callIncommingEvent.js";
import callAcceptEvent from "./socket-event/callAcceptEvent.js";
import typingEvent from "./socket-event/typingEvent.js";

import { Redis } from "ioredis";
// import {startMessageConsumer} from './kafka/message-consumer.js'
import { createClient } from "redis";
import { createAdapter } from "@socket.io/redis-streams-adapter";
import callRejectEvent from "./socket-event/callRejectEvent.js";
import unsentMessage from "./socket-event/unsentMessage.js";

import { validateSecureToken } from "./services/tokenService.js";



// // Generate a random encryption key with the desired length (256 bits / 32 bytes)
// const encryptionKey = crypto.randomBytes(32);

// // Encryption function
// function encryptMessage(message, key) {
//     const iv = crypto.randomBytes(16); // Generate Initialization Vector (IV)
//     const cipher = crypto.createCipheriv('aes-256-cbc', key, iv);
//     let encrypted = cipher.update(message, 'utf8', 'hex');
//     encrypted += cipher.final('hex');
//     return iv.toString('hex') + ':' + encrypted;
// }

// // Decryption function
// function decryptMessage(encryptedMessage, key) {
//     const parts = encryptedMessage.split(':');
//     const iv = Buffer.from(parts[0], 'hex');
//     const decipher = crypto.createDecipheriv('aes-256-cbc', key, iv);
//     let decrypted = decipher.update(parts[1], 'hex', 'utf8');
//     decrypted += decipher.final('utf8');
//     return decrypted;
// }

// // Example usage
// const message = 'Hello, world!ihiuhihouhuijhfgjhfvjhfgyugfuyfgkyufkuyfkuy';
// const encryptedMessage = encryptMessage(message, encryptionKey);
// console.log('Encrypted Message:', encryptedMessage);

// const decryptedMessage = decryptMessage(encryptedMessage, encryptionKey);
// console.log('Decrypted Message:', decryptedMessage);
 





const app = express()
//env for secret credentials
const port = process.env.PORT || 1024

app.use(cookieParser());

// app.use(session({
//   name:'session',
//   secret: 'your-secret-key',
//   resave: false,
//   saveUninitialized: true, 
//   cookie:{
//     maxAge:2 * 60 * 1000,
//    httpOnly:true,

//   } 
// }));   

// await redisClient.connect()

const server = http.createServer(app);

const io = new Server(server, {
  adapter: createAdapter(redisClient, {
    maxLen: 1000
  }),
  
  maxHttpBufferSize: 1e8,
  cors: {
    origin: "*",
    methods: ["GET", "POST"],
    credentials:true
  },
});


// app.use(cors({ credentials: true, origin: 'http://localhost:3000', methods: ["GET", "POST", "PUT", "DELETE"],}));

app.use(bodyParser.urlencoded({ extended: true }));

// Trust the first proxy
app.set('trust proxy', true);

//json formate 
app.use(express.json())

//disable x-powered-by;
app.disable('x-powered-by');


const __dirname = dirname(fileURLToPath(import.meta.url))

// Serve your React app here
app.use(express.static(path.join(__dirname, 'public')));


// Specify the directory where the images are stored
app.use('/uploads', express.static('uploads'));
app.use('/media', express.static('media'));




app.use('/public', express.static('public'));

//api routes
app.use('/api', router )


// recommendation() 



const otpTimeouts = {};

const scheduleTaskForUser = (email, remainingTime) => {
  // Clear existing timeout for this user, if any
  otpTimeouts[email] = setTimeout(() => {
    console.log(`Executing scheduled task for user with email ${email}...`);
    userOtp.findOneAndDelete({ email: email }, function (err, docs) {
      if (err) {
        console.log(err);
      } else { 
        console.log("Deleted User:", docs);
      }
    });
  }, remainingTime);
  console.log(otpTimeouts);
};

const restoreTimeoutsFromDatabase = async () => {
  try {
    // Create a streaming cursor to fetch users with OTPs from the database
    const cursor = userOtp.find().cursor();
    
    // Iterate over each user asynchronously
    for await (const user of cursor) {
      const currentTime = Date.now(); 
      const expirationTime = user.date.getTime(); // Assuming 'date' is a Date field in your schema
      const remainingTime = expirationTime - currentTime  
      console.log(remainingTime, 'expirationTime'); 
      if (remainingTime > 0) {
        scheduleTaskForUser(user.email, remainingTime); 
      } else { 
        console.log(`OTP for user with email ${user.email} has already expired.`);
    
        userOtp.findOneAndDelete({ email: user.email }, function (err, docs) {
          if (err) {
            console.log(err);     
          } else {
            clearTimeout(otpTimeouts[user.email]); // Fix this line to use user.email instead of undefined email
             console.log("Deleted User:", docs);
          }
        });
      } 
    }
  } catch (error) {
    console.error("Error restoring timeouts:", error);
  }
}; 

restoreTimeoutsFromDatabase();

 
// const pub = new Redis();
// const sub = new Redis();


// startMessageConsumer()

//socket

let userData = [];

io.use(async(socket,  next) => {
  // do something


const {token} = socket.handshake.auth
const {userID} = socket.handshake.query
  
try {
  // Verify the JWT token

const result2 = validateSecureToken(token,  process.env.JWT_SECRET).payload
// console.log(result2, 'result2')

  // const result = jwt.verify(token, process.env.JWT_SECRET);



  // Check if the token is valid and matches the user ID
  if (result2.id === userID) {
    //  next();     // Proceed with the socket connection


     const {username, email} = await userschema.findById(result2.id)


     const disable = await disableSchema.findOne({
                
         $or: [{ identifier: username, }, { email: email }],
       });


     if(disable !== null){
console.log('id disable')
     }else{
      next();
     }



  } else {
    throw new Error('Invalid token or user ID mismatch');
  }
} catch (error) {
  // Handle authentication errors 
  console.error('Authentication error:', error.message);
  next(error); // Pass the error to the next middleware or handler
}

});





const ONLINE_USERS_KEY = 'online_users';
const LAST_ACTIVE =  'last_active'
const IS_USER_CALLING = 'isUserCalling'


io.on("connection", async (socket) => {
  //console log when user is connected
  console.log("a user connected", socket.id);
  // await redisClient.subscribe('onlineUser')


  async function setUserOnline(userId, socketId) {
    await redisClient.hset(ONLINE_USERS_KEY, userId, socketId);
    const result = await redisClient.hdel(LAST_ACTIVE, userId);
    // console.log(`User ${userId} is online with socket ${socketId}`);
  }


  async function isUserOnline(userId) {
    const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
    return _userId;
  }


  async function checkUserStatus(userId) {
    const _userId = await isUserOnline(userId);
    if (_userId) {
      // console.log(`User ${userId} is online with socket ${userId} tamjid`);


        socket.emit('userOnline', {
        active: 'online' ,
        Id:socket.handshake.query.userID
      })


    }
  }

 await checkUserStatus(socket.handshake.query.userID)
await setUserOnline(socket.handshake.query.userID, socket.id)



  try {
    // const userFind = await onlineSchema.findOne({ id: socket.handshake.query.userID });

 
 

    // if (userFind !== null) {
     
    //   // const lastActive = await lastActiveSchema.findOneAndDelete({Id: userFind.id})

    
 
    //   await onlineSchema.updateMany({ id: socket.handshake.query.userID }, { socketId: socket.id }, { new: true });
    // } else {
    //   // console.log("no data");
    //   const online = new onlineSchema({
    //     socketId: socket.id,
    //     id: socket.handshake.query.userID,
    //   });

    //   online.save();


    //   socket.emit('userOnline', {
    //     active: 'online' ,
    //     Id:socket.handshake.query.userID
    //   })



    // }



    await lastActiveSchema.findOneAndDelete({Id:socket.handshake.query.userID})
socket.on('activeStatus', async(data)=>{
  // console.log(data, 'activestatius')



  const activeStatus = await lastActiveSchema.findOne({Id: data.userId})



if(activeStatus !== null){
  // console.log(activeStatus, '332 last active')

  socket.emit('lastactive', {
    active: activeStatus.lastActive
  })
  
}else{
  socket.emit('lastactive', {
    active: 'online' 
  })







}









})




  } catch (error) {
    console.log(error);
  }


 

//send message event function
  sendMessageEvent({io, socket, ONLINE_USERS_KEY})

//request accept event function
  requestAcceptEvent({io, socket, ONLINE_USERS_KEY })

//incomming call event
  incommingCallEvent({io, socket, ONLINE_USERS_KEY, IS_USER_CALLING})

// call accepted event
callAcceptEvent({io, socket, ONLINE_USERS_KEY})
//
callRejectEvent({io, socket, IS_USER_CALLING, ONLINE_USERS_KEY})


typingEvent({io, socket,  ONLINE_USERS_KEY})

unsentMessage({io, socket, ONLINE_USERS_KEY})

 



  socket.on("seen", (data) => {
    // const find = userData.find(obj =>obj.id === data.receiverId)

    // if(find){
    //   io.to(find.socketId).emit('seen', data)
    // }

    // console.log(data);
  });



  const __dirname = path.dirname(fileURLToPath(import.meta.url));
    // Define the folder where you want to save the uploaded images
    const baseFolder = path.join(__dirname, `media`);
    // Create the uploads folder if it doesn't exist
    if (!fs.existsSync(baseFolder)) {
      fs.mkdirSync(baseFolder);
    } 


const fileSave = async( folder,  data, senderId, receiverId, type, profile) =>{

  const folderPath = path.join(baseFolder, folder);
    // Create the folder if it doesn't exist
    if (!fs.existsSync(folderPath)) {
      fs.mkdirSync(folderPath, { recursive: true }); // recursive: true creates parent folders if they don't exist
    }
 
    const filename = `${Date.now()}_${Math.floor(Math.random() * 1000)}.jpg`;
  
  
    // Create a writable stream to save the image data to a file
    const filePath = path.join(folderPath, filename);

    const parentFolder = path.basename(path.dirname(path.dirname(filePath)));

    const childFolder = path.basename(path.dirname(filePath));

    const writeStream = fs.createWriteStream(filePath);
  


const link =  `${parentFolder}/${childFolder}/${filename}`

    const insertData = await new messageSchema({
      senderId:senderId,
      receiverId:receiverId,
      textFor:receiverId,
      text:link,
      type:type
    }).save()
    const insertData2 = await new messageSchema({
      senderId:senderId,
      receiverId:receiverId,
      textFor:senderId,
      text:link,
      type:type
    }).save()

      // Write the image data to the file 
      writeStream.write(Buffer.from(data));
      writeStream.end();

// console.log(insertData, 'saved to inboxmedia')
  
    // console.log('Image saved to:', `${parentFolder}/${childFolder}/${filename}`);


    const onlineData = await onlineSchema.findOne({ id: receiverId });


    io.to(onlineData.socketId).emit('imageReceive', {
     
      profile: profile,
      iSend: senderId,
      whoSend: receiverId, 
      text:link,
      socketId: socket.id,
      type:'image',
      
    })


    socket.emit('outgoingMedia', {
      iSend: senderId,
        text: link,
        socketId: socket.id,
        receiverId: receiverId,
        type:'image',
    })


 
}
 

//upload file in inbox
  socket.on('uploadImage', async(data)=>{
    // console.log(data) 
   
// console.log(data)

  if(data.type === 'image/jpeg'){
    fileSave(`images`, data.imageData, data.senderId, data.receiverId , 'image', data.profile )  
  }



  })


  socket.on('uploadAudio', async(data)=>{
    // console.log(data)
    if(data.type === 'audio/mpeg'){



      const folderPath = path.join(baseFolder, 'audio');
      // Create the folder if it doesn't exist
      if (!fs.existsSync(folderPath)) {
        fs.mkdirSync(folderPath, { recursive: true }); // recursive: true creates parent folders if they don't exist
      }
   
      const filename = `${Date.now()}_${Math.floor(Math.random() * 1000)}.mp3`;
    
    
      // Create a writable stream to save the image data to a file
      const filePath = path.join(folderPath, filename);
  
      const parentFolder = path.basename(path.dirname(path.dirname(filePath)));
  
      const childFolder = path.basename(path.dirname(filePath));
  
      const writeStream = fs.createWriteStream(filePath);
    
  
  
  const link =  `${parentFolder}/${childFolder}/${filename}`
  
      const insertData = await new messageSchema({
        senderId:data.senderId,
        receiverId:data.receiverId,
        text:link,
        type:'audio'
      }).save()
  
        // Write the image data to the file 
        writeStream.write(Buffer.from(data.audioData));
        writeStream.end();
  
        try {
          const onlineData = await onlineSchema.findOne({ id: data.receiverId });

          io.to(onlineData.socketId).emit('audioReceive', {
       
            profile: data.profile,
            iSend: data.senderId,
            whoSend: data.receiverId, 
            text:link,
            socketId: socket.id,
            type:'audio',
            audioName: data.name
            
          })
        } catch (error) {
          console.log(error)
        }

  


        socket.emit('outgoingAudioMedia', {
          iSend: data.senderId,
            text: link,
            socketId: socket.id,
            receiverId: data.receiverId,
            type:'audio',
        })




    }  
  })


  // socket.on('onScreen' , async(data)=>{
  //   console.log(data.receiverId, 'onscreen')

  //   try {
  //     const onlineData = await onlineSchema.findOne({ id: data.receiverId });



  //       io.to(onlineData.socketId).emit('onScreenReceive', {
  //         senderId:data.senderId,
  //         receiverId:data.receiverId,
  //         onscreen:true
  //       })
    


    
  //   } catch (error) {
  //     console.log('offline')
  //   }

  


  // })





  // if user disconnect
  socket.on("disconnect", async () => {
    console.log("a user disconnected", socket.id);


    


    // const userRemove = await onlineSchema.findOneAndDelete({ socketId: socket.id });


    // Remove user online status by socket ID
async function setUserOffline(userId) {
  console.log(`Attempting to set socket ${userId} offline`);
   await redisClient.hdel(ONLINE_USERS_KEY, userId);
await redisClient.hset(LAST_ACTIVE, socket.handshake.query.userID , new Date().toISOString())

}

  await setUserOffline(socket.handshake.query.userID)

  io.emit('userDisconnect', {
    data : 'user is disconnected'
  })



  
   

    // console.log(userRemove, 'user remove');

    // if (userRemove) {
    //   console.log(userRemove.id, 'user removed');


      // const checkActives = await lastActiveSchema.findOne({Id:userRemove.id})

// console.log(checkActives, 'check actives')
// if(checkActives === null){
//   const lastActive = await new lastActiveSchema({
//     Id : userRemove.id,

//   }).save()
// }
     
   
    
   
    
        // socket.emit('lastActive', {
        // id: 'kjj'
        // //  active : lastActive.lastActive
        // })
      






  // } else {
  //     console.log('User with socket ID', socket.id, 'not found or already removed');
  // }







    const userIdToRemove = socket.id;
    const indexToRemove = userData.findIndex((obj) => obj.socketId === userIdToRemove);

    if (indexToRemove !== -1) {
      userData.splice(indexToRemove, 1);
      console.log(`Object with userId ${userIdToRemove} removed`);
    } else {
      console.log(`No object found with userId ${userIdToRemove}`);
    }

    // console.log(userData);
  });
});



















// app.use((req, res, next) => {
//     res.header('Access-Control-Allow-Origin', '*');
//     res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
//     // res.header('Access-Control-Allow-Credentials', 'true');
//     next();
//   });





// Define the catch-all route for all other requests
// app.get('*', (req, res) => {
//   res.status(404).sendFile(path.join(__dirname, 'public/error.html'));
// });




//use session for security
// app.use(session({
//     key: 'login', 
//     secret: process.env.SESSION_SECRET ,
//     resave: false,
//     saveUninitialized: true,
//     cookie:{secure:true, maxAge: 15*60*1000, httpOnly:true}
    
// }))


// use cors
// app.use(cors({
//   origin: 'http://localhost:3000',
//   methods:['GET', 'POST'],
//   credentials:true, 
  
// }))


// app.use(cors())

// app.get('/',(req, res)=>{
//   res.cookie('name', 'tamjid', {sameSite:'strict', path:'/', expires:new Date(new Date().getTime() + 1 * 60 * 1000)})
// })


// app.use((req, res, next) => {
//     console.log(req.ip)
//     next()
//   })
  




// app.use((req, res, next) => {
//     if (req.cookies.user_id && !req.session.user_id) {
//       res.clearCookie("login");
//     }
//     next();
//   });





//start express app

server.listen(port , async()=>{
  console.log(`server connected to port ${port}`)

const db_connection = await connectDB()
   
if(db_connection){
console.log('mongodb connected')
}else{
  console.log('mongodb not connected')
}


})


// connectDB().then(()=>{
//  server.listen(port, ()=>{
//     console.log(`mongodb connected and server connected to port ${port}`)
//    }) 
// }).catch((error)=>{
//     console.log('invalid port',error)
// })
