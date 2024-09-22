import fs from "fs";
import nodemailerConfig from "./mail-config.js";

const sendMail = ({ email = null, subject = null, template = null, otp = null, name = null, ip = null, os = null, browser = null, location = null, device = null, time = null }) => {
  const mailOptions = {
    from: `WhispTalk <${process.env.GMAIL}>`,
    to: `${email}`,
    subject: subject,
  };
  // Read the HTML template file
  fs.readFile(template, "utf-8", (err, data) => {
    if (err) {
      console.error(err);
      return;
    }
    // Replace placeholders in the template with actual OTP and name

    if ((otp && name) !== null) {
      mailOptions.html = data.replace(/{OTP}/g, otp).replace(/{NAME}/g, name);
    } else if ((ip && browser && location && device & time) !== null) {
      mailOptions.html = data
        .replace(/{name}/g, name)
        .replace(/{ip}/g, ip)

        .replace(/{browser}/g, browser)
        .replace(/{location}/g, location)
        .replace(/{device}/g, device)
        .replace(/{time}/g, time);
    } else {
      mailOptions.html = data.replace(/{NAME}/g, name);
    }

    // Send mail
    nodemailerConfig.transporter.sendMail(mailOptions, (error, info) => {
      if (error) {
        console.log(error);
      } else {
        console.log("Email sent: " + info.response);
      }
    });
  });
};

export default sendMail;
