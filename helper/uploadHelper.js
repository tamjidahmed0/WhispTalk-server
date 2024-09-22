import fs from "fs";

const uploadHelper = (req, file, cb, uploadFolder) => {
  const mainFunc = (foldername) => {
    // Check if the main directory exists
    if (!fs.existsSync(`${uploadFolder}/${req.params.id}`)) {
      // Create the main directory if it doesn't exist
      fs.mkdirSync(`${uploadFolder}/${req.params.id}`, { recursive: true });

      // Check if the subdirectory exists
      if (!fs.existsSync(`${uploadFolder}/${req.params.id}/${foldername}`)) {
        // Create the subdirectory if it doesn't exist
        fs.mkdirSync(`${uploadFolder}/${req.params.id}/${foldername}`, { recursive: true });

        // Check if the query type matches the foldername
        if (req.query.type === `${foldername}`) {
          cb(null, `${uploadFolder}/${req.params.id}/${foldername}`);
        }
      }
    } else {
      // The main directory exists

      // Check if the subdirectory exists
      if (!fs.existsSync(`${uploadFolder}/${req.params.id}/${foldername}`)) {
        // Create the subdirectory if it doesn't exist
        fs.mkdirSync(`${uploadFolder}/${req.params.id}/${foldername}`, { recursive: true });

        // Check if the query type matches the foldername
        if (req.query.type === `${foldername}`) {
          cb(null, `${uploadFolder}/${req.params.id}/${foldername}`);
        }
      } else {
        // The subdirectory exists

        // Check if the query type matches the foldername
        if (req.query.type === `${foldername}`) {
          cb(null, `${uploadFolder}/${req.params.id}/${foldername}`);
        }
      }
    }
  };

  //check all condition
  if (req.query.type === "profile") {
    mainFunc("profile");
  } else if (req.query.type === "post") {
    mainFunc("post");
  } else if (req.query.type === "stories") {
    mainFunc("stories");
  } else if (req.query.type === "videos") {
    mainFunc("videos");
  }
};

export default uploadHelper;
