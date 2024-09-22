import multer from "multer";
import path from "path";
import fs from "fs";
import userSchema from "../models/user.js";
import { uploadHelper } from "../helper/index.js";

const UploadImage = (req, res, next) => {
  const uploadFolder = "uploads";

  const storage = multer.diskStorage({
    destination: (req, file, cb) => {
      console.log(req.query);

      // Create the uploads folder if it doesn't exist
      if (!fs.existsSync(uploadFolder)) {
        fs.mkdirSync(uploadFolder, { recursive: true });

        //upload helper function call
        uploadHelper(req, file, cb, uploadFolder);
      } else {
        //upload helper function call
        uploadHelper(req, file, cb, uploadFolder);
      }
    },
    filename: (req, file, cb) => {
      const fileExt = path.extname(file.originalname);
      const uniqueFileName = req.params.id + "-" + Date.now() + fileExt;
      cb(null, uniqueFileName);
    },
  });

  const upload = multer({
    storage: storage,
    limits: { fileSize: 1000000000 },
    fileFilter: (req, file, cb) => {
      if (file.mimetype === "image/png" || file.mimetype === "image/jpg" || file.mimetype === "image/jpeg") {
        cb(null, true);
      } else {
        cb(new Error("only jpg, jpeg and png file allowed!"));
      }
    },
  }).single("image");

  const validations = async () => {
    const id = req.params.id;
    try {
      const user = await userSchema.findById(id);

      if (user) {
        upload(req, res, (err) => {
          if (err) {
            console.log(err);
            console.log(err.code);
            if (err.code === "LIMIT_FILE_SIZE") {
              return res.status(500).json({ msg: "File is Too large" });
            }
            return res.status(500).json({ msg: "Failed to upload file" });
          }

          // If everything is fine, call next() to pass control to the next middleware
          next();
        });
      } else {
        return res.status(400).json({ msg: "user does not exist" });
      }
    } catch (error) {
      console.log(error);
    }
  };

  validations();
};

export default UploadImage;
