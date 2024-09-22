      // Function to schedule OTP deletion
import userOtp from '../models/otp.js'

      const otpTimeouts = {};

      const scheduleTaskForUser = (email) => {
        clearTimeout(otpTimeouts[email]); // Clear existing timeout for this user, if any
        const timeoutId = setTimeout(() => {
          console.log(`Executing scheduled task for user with email ${email}...`);
          userOtp.findOneAndDelete({ email: email }, function (err, docs) {
            if (err) {
              console.log(err);
            } else {
              console.log("Deleted User:", docs);
            }
          }); 
        }, 180000); // 3 min delay 180000

        otpTimeouts[email] = timeoutId; // Store the timeout ID
      
      };

      

export {scheduleTaskForUser , otpTimeouts}