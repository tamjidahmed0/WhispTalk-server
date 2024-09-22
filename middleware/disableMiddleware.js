import disableSchema from "../models/disable.js";
import userSchema from "../models/user.js";

const disableMiddleware = async (req, res, next) => {
  const { identifier, id } = req.body;
  const paramsId = req.params.userId;

  try {
    if (paramsId === undefined) {
      const disable = await disableSchema.findOne({
        $or: [{ identifier: identifier }, { email: identifier }],
      });

      // console.log(disable, "disable");
      if (disable) {
        res.status(400).send({ title: disable.Title, text: disable.Text, status: 400 });
      } else {
        next();
      }
    } else {
      const { username, email } = await userSchema.findById(paramsId);

      const disable = await disableSchema.findOne({
        $or: [{ identifier: username }, { email: email }],
      });

      // console.log(disable, "disable");
      if (disable) {
        res.status(401).send({ title: disable.Title, text: disable.Text, status: 401 });
      } else {
        next();
      }
    }
  } catch (error) {
    console.log(error);
  }
};

export default disableMiddleware;
