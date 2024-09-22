import userSchema from "../models/user.js";
import bcrypt from "bcrypt";

const ChangePassword = async (req, res) => {
  const userId = req.params.userId;
  const { currentPass, newPass } = req.body;

  try {
    // Find the user by ID
    const user = await userSchema.findById(userId);

    // Check if the user exists
    if (!user) {
      return res.status(404).json({ msg: "User not found" });
    }

    // Compare the current password
    const isMatch = await bcrypt.compare(currentPass, user.password);
    if (!isMatch) {
      return res.status(400).json({ msg: "Current password is incorrect" });
    }

    if (newPass.length > 6) {
      // Hash the new password
      const salt = await bcrypt.genSalt(10);
      const hashedPassword = await bcrypt.hash(newPass, salt);

      // Update the password field
      const updatedUser = await userSchema.findByIdAndUpdate(userId, { password: hashedPassword }, { new: true });
      console.log(updatedUser);
      if (updatedUser) {
        return res.status(201).json({ msg: "Password changed successfully!", status: 201 });
      } else {
        return res.status(500).json({ msg: "Password update failed" });
      }
    } else {
      return res.status(400).json({ msg: "Need atleast 6 character long!" });
    }
  } catch (error) {
    console.error(error);
    return res.status(500).json({ msg: "Server error" });
  }
};

export default ChangePassword;
