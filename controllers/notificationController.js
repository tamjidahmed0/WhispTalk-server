import notificationSchema from "../models/notification.js";
import profileSchema from "../models/profile.js";
import moment from "moment";

const notificationController = async (req, res) => {
  const userId = req.params.userId;
  try {
    const notification = await notificationSchema.find({
      receiverId: userId,
    });

    const count = await notificationSchema.countDocuments({
        receiverId: userId,
    })

    // console.log(notification);

    let data = [];

    await Promise.all(
      notification.map(async (item) => {
        const createdAt = moment(item.date);
        const now = moment();
        const duration = moment.duration(now.diff(createdAt));

        const years = duration.years();
        const months = duration.months();
        const days = duration.days();
        const hours = duration.hours();
        const minutes = duration.minutes();

        let timeAgo;

        if (years > 0) {
          timeAgo = `${years} ${years === 1 ? "year" : "years"} ago`;
        } else if (months > 0) {
          timeAgo = `${months} ${months === 1 ? "month" : "months"} ago`;
        } else if (days > 0) {
          timeAgo = `${days} ${days === 1 ? "day" : "days"} ago`;
        } else if (hours > 0) {
          timeAgo = `${hours} ${hours === 1 ? "hour" : "hours"} ago`;
        } else if (minutes > 0) {
          timeAgo = `${minutes} ${minutes === 1 ? "minute" : "minutes"} ago`;
        } else {
          timeAgo = "Just now";
        }

        const { profilePic } = await profileSchema.findOne({
          Id: item.senderId,
        });

        data.push({
          profile: profilePic,
          msg: item.text,
          time: timeAgo,
        });
      })
    );

    res.status(200).send({count:count, data});
  } catch (error) {}
};

export default notificationController;
