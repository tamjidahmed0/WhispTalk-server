import { Reader } from "@maxmind/geoip2-node";
import useragent from "useragent";
import sendMail from "../mail/sendMail.js";
import path from "path";
import { fileURLToPath } from "url";
import { dirname } from "path";
import maxmind from "maxmind";
import fs from "fs";
import securitySchema from "../models/security-login.js";
import userSchema from "../models/user.js";

const securityAlert = async (req, res, next) => {
  const { identifier } = req.body;
  const forwarded = req.headers["x-forwarded-for"];
  const source = req.headers["user-agent"];
  const agent = useragent.parse(source);
  const ip = forwarded ? forwarded.split(",").shift() : req.ip;

  const __dirname = dirname(fileURLToPath(import.meta.url));

  // Path to the GeoLite2 database file
  const dbPath = path.join(__dirname, "../geolite/GeoLite2-City.mmdb");

  //   // Open the GeoLite2 database
  //   Reader.open(dbPath).then(reader => {
  //     console.log(reader.city(ip));
  //   });

  /*
This function is for access the public ip. Because we don't access our ip from localhost.
This is acctually a thirt party api if in production you can remove this fuction.
*/
  async function getPublicIP() {
    try {
      const response = await fetch("https://api.ipify.org?format=json");
      const data = await response.json();
      return data.ip;
    } catch (error) {
      console.error("Error fetching public IP:", error);
      return "Unable to fetch public IP";
    }
  }
  // async function getmmdb() {
  //   try {
  //     const response = await fetch('https://firebasestorage.googleapis.com/v0/b/test-6c839.appspot.com/o/profiles%2FGeoLite2-City.mmdb?alt=media&token=f924b381-f2fb-4aef-a250-169558526481');
  //     const data = await response.json();
  //     return data.ip;
  //   } catch (error) {
  //     console.error('Error fetching public IP:', error);
  //     return 'Unable to fetch public IP';
  //   }
  // }

  // const mmdb = await getmmdb()

  // console.log(mmdb, 'mmdb')

  const publicIp = await getPublicIP();
  console.log(`Public IP: ${publicIp}`);

  // Function to determine device type
  const getDeviceType = (userAgent) => {
    if (/mobile/i.test(userAgent)) {
      return "Mobile";
    }
    if (/tablet/i.test(userAgent)) {
      return "Tablet";
    }
    if (/iPad|Android|PlayBook|Silk|Kindle|iPod/i.test(userAgent)) {
      return "Tablet";
    }
    if (/iPhone|Android|Windows Phone|BlackBerry/i.test(userAgent)) {
      return "Mobile";
    }
    if (/Windows NT 10.0/i.test(userAgent)) {
      return "Desktop";
    }
    return "unknown";
  };

  // Function to determine operating system and version
  const getOS = (userAgent) => {
    let os = "Unknown OS";
    let osVersion = "Unknown Version";

    if (/Windows NT 10.0/i.test(userAgent)) {
      os = "Windows";
      osVersion = "10";
    } else if (/Windows NT 6.2/i.test(userAgent)) {
      os = "Windows";
      osVersion = "8";
    } else if (/Windows NT 6.1/i.test(userAgent)) {
      os = "Windows";
      osVersion = "7";
    } else if (/Windows NT 6.0/i.test(userAgent)) {
      os = "Windows";
      osVersion = "Vista";
    } else if (/Windows NT 5.1|Windows XP/i.test(userAgent)) {
      os = "Windows";
      osVersion = "XP";
    } else if (/Mac OS X 10[._]\d+/i.test(userAgent)) {
      os = "Mac OS";
      const match = userAgent.match(/Mac OS X 10[._]\d+/i);
      if (match) {
        osVersion = match[0].replace("Mac OS X ", "").replace("_", ".");
      }
    } else if (/Android \d+/i.test(userAgent)) {
      os = "Android";
      const match = userAgent.match(/Android \d+/i);
      if (match) {
        osVersion = match[0].replace("Android ", "");
      }
    } else if (/iPhone OS \d+_\d+/i.test(userAgent)) {
      os = "iOS";
      const match = userAgent.match(/iPhone OS \d+_\d+/i);
      if (match) {
        osVersion = match[0].replace("iPhone OS ", "").replace("_", ".");
      }
    } else if (/iPad.*OS \d+_\d+/i.test(userAgent)) {
      os = "iOS";
      const match = userAgent.match(/OS \d+_\d+/i);
      if (match) {
        osVersion = match[0].replace("OS ", "").replace("_", ".");
      }
    }

    return { os, osVersion };
  };

  // const getOS = (userAgent) => {
  //   let os = 'Unknown OS';
  //   let osVersion = 'Unknown Version';
  //   let brand = 'Unknown';
  //   let model = 'Unknown';

  //   if (/Windows NT 10.0/i.test(userAgent)) {
  //     os = 'Windows';
  //     osVersion = '10';
  //   } else if (/Windows NT 6.2/i.test(userAgent)) {
  //     os = 'Windows';
  //     osVersion = '8';
  //   } else if (/Windows NT 6.1/i.test(userAgent)) {
  //     os = 'Windows';
  //     osVersion = '7';
  //   } else if (/Windows NT 6.0/i.test(userAgent)) {
  //     os = 'Windows';
  //     osVersion = 'Vista';
  //   } else if (/Windows NT 5.1|Windows XP/i.test(userAgent)) {
  //     os = 'Windows';
  //     osVersion = 'XP';
  //   } else if (/Mac OS X 10[._]\d+/i.test(userAgent)) {
  //     os = 'Mac OS';
  //     const match = userAgent.match(/Mac OS X 10[._]\d+/i);
  //     if (match) {
  //       osVersion = match[0].replace('Mac OS X ', '').replace('_', '.');
  //     }
  //   } else if (/Android \d+/i.test(userAgent)) {
  //     os = 'Android';
  //     const match = userAgent.match(/Android \d+/i);
  //     if (match) {
  //       osVersion = match[0].replace('Android ', '');
  //     }
  //     // Dynamically extract brand and model number
  //     const brandModelMatch = userAgent.match(/\(([^;]+); ([^;]+)\)/);
  //     if (brandModelMatch) {
  //       const brandInfo = brandModelMatch[1].split(';')[0].trim();
  //       const modelInfo = brandModelMatch[2].trim();
  //       brand = brandInfo.split(' ')[0];
  //       model = modelInfo;
  //     }
  //   } else if (/iPhone OS \d+_\d+/i.test(userAgent)) {
  //     os = 'iOS';
  //     const match = userAgent.match(/iPhone OS \d+_\d+/i);
  //     if (match) {
  //       osVersion = match[0].replace('iPhone OS ', '').replace('_', '.');
  //     }
  //   } else if (/iPad.*OS \d+_\d+/i.test(userAgent)) {
  //     os = 'iOS';
  //     const match = userAgent.match(/OS \d+_\d+/i);
  //     if (match) {
  //       osVersion = match[0].replace('OS ', '').replace('_', '.');
  //     }
  //   }

  //   return { os, osVersion, brand, model };
  // };

  // Function to determine browser type
  const getBrowserType = (userAgent) => {
    if (/chrome|crios|crmo/i.test(userAgent)) {
      return "Chrome";
    }
    if (/firefox|fxios/i.test(userAgent)) {
      return "Firefox";
    }
    if (/safari/i.test(userAgent) && !/chrome|crios|crmo/i.test(userAgent)) {
      return "Safari";
    }
    if (/edg/i.test(userAgent)) {
      return "Edge";
    }
    if (/opera|opr\//i.test(userAgent)) {
      return "Opera";
    }
    if (/msie|trident/i.test(userAgent)) {
      return "Internet Explorer";
    }
    return "Unknown";
  };

  // const getModel = (userAgent) => {
  //   // Regular expression to match model after "Android" and before "Build"
  //   const modelRegex = /Android \d+;([^)]+)/i;

  //   // Match model using regex
  //   const match = userAgent.match(modelRegex);

  //   if (match && match[1]) {
  //     const modelInfo = match[1].trim();
  //     // Check if "Build" is available, if not, return everything after "Android"
  //     if (modelInfo.includes('Build')) {
  //       return modelInfo.split('Build')[0].trim();
  //     } else {
  //       return modelInfo;
  //     }
  //   } else {
  //     return 'Model not found';
  //   }
  // };

  // Get the device model from user-agent
  // const deviceModel = getModel(source);

  const browserType = getBrowserType(source);
  const { os, osVersion, brand } = getOS(source);

  console.log(browserType, "browsertype");
  console.log(os, "os");
  console.log(osVersion, "os version");

  try {
    const reader = await Reader.open(dbPath);
    const country = reader.city(publicIp).country.names.en;
    const city = reader.city(publicIp).city === undefined ? country : reader.city(publicIp).city.names.en;
    // console.log(reader.city(publicIp).country.names.en)
    // console.log(reader.city(publicIp).city.names.en)
    // console.log(agent.device.toString())
    // console.log(agent.toAgent())
    // console.log(ip)

    // next()
    // console.log(reader.city(publicIp), 'country')
    // console.log(city, 'city')

    const user = await userSchema.findOne({ username: identifier });
    if (user) {
      const check_Login = await securitySchema.find(user._id);

      console.log(check_Login, "check login");

      const add_login_doc = await new securitySchema({
        Id: user._id,
        ip: publicIp,
        device: `${os} ${osVersion}`,
        app: browserType,
        location: `${city}, ${country}`,
      }).save();

      // console.log(add_login_doc, 'add login doc')
      const specificDate = new Date(); // Note: Months are 0-indexed in JavaScript

      // Format the date to a readable string
      const options = { year: "numeric", month: "long", day: "numeric" };
      const formattedDate = specificDate.toLocaleDateString("en-US", options);

      // sendMail({
      //   email : user.email,
      //   subject: "Login alert! : Someone logged in to your account.",
      //   name:user.name,
      //   template: './mail-template/security-alert.html',
      //   ip:publicIp,
      //   browser: browserType,
      //   device: `${os} ${osVersion}`,
      //   location: `${city}, ${country}`,
      //   time: formattedDate
      // })

      next();
    }

    // res.status(208).send({
    //   country: reader.city(publicIp).country.names.en,
    //   city : reader.city(publicIp).city.names.en,
    //   os :os,
    //   version: osVersion
    // })

    //   sendMail({
    //     email: user.email,
    //     subject: 'We notice a new login',
    //     template: './mail-template/security-alert.html',
    //     name: user.name,
    //     ip: '180.149.232.168',
    //     os:'windows 10',
    //     location: 'Bangladesh',
    //     browser: 'chrome',
    //     device: 'desktop',
    //     time: '2 October 2024'
    //   })
  } catch (error) {
    console.log(error);
  }
};

export default securityAlert;
