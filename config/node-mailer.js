import nodemailer from 'nodemailer'
import * as dotenv from 'dotenv' 
dotenv.config()

// const transporter = nodemailer.createTransport({
//     service: 'gmail',
//     auth: {
//       user: process.env.GMAIL,
//       pass: process.env.GMAIL_PASS
//     }
//   })

const transporter = nodemailer.createTransport({
  host: 'smtp.gmail.com',
  port: 587,
  secure: false, // Use false if your server doesn't use SSL/TLS
  auth: {
    user: process.env.GMAIL,
    pass: process.env.GMAIL_PASS,
  },
});



export default {transporter}