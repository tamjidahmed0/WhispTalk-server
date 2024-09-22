import conversationSchema from "../models/conversation.js";
import userSchema from "../models/user.js";
import profileSchema from "../models/profile.js";
import moment from "moment";
import unReadSchema from "../models/unReadMsg.js";

import { ObjectId } from "mongodb";

const searchController = async (req, res) => {
  const userId = req.params.userId;
  const searchTerm = req.query.name;

  try {
    const Regex = new RegExp(`^${searchTerm}`, "i");

    const conversation = await conversationSchema.find({
      conversationFor: userId,
    });

    const unread = await unReadSchema.find({ receiverId: userId });

    const countsMap = new Map(unread.map(({ senderId, count }) => [senderId.toString(), count]));

    let names = [];

    await Promise.all(
      conversation.map(async (item) => {
        const createdAt = moment(item.date);
        const now = moment();
        const duration = moment.duration(now.diff(createdAt));

        const years = duration.years();
        const months = duration.months();
        const days = duration.days();

        const date = moment(item.date);
        
        const getFormattedDate = (date) => {
          if (Math.abs(years)) {
            return `${date.format("MMM DD YYYY")}`;
          } else if (Math.abs(months)) {
            return `${date.format("MMM DD")}`;
          } else if (days) {
            return `${date.format("ddd")}`;
          } else {
            return date.format("hh:mm A");
          }
        };

        let profiles;

        if (item.members[0].toString() === userId) {
          profiles = await profileSchema.findOne({
            Id: item.members[1],
          });
        }

        if (item.members[1].toString() === userId) {
          profiles = await profileSchema.findOne({
            Id: item.members[0],
          });
        }

        // console.log(profiles.profilePic, 'come from profile')

        const profilePic = profiles ? profiles.profilePic : "";
        // console.log(profiless)

        names.push({
          profile: profilePic,
          name: item.receiverName,
          verified:item.isVerified,
          Id: item.members[1],
          text: item.text,
          date: getFormattedDate(date),
        });
      })
    );

    const filterUser = names.filter((user) => Regex.test(user.name));

    const mergedArray = filterUser.map((user) => ({
      ...user,
      unReadMsgCount: countsMap.get(user.Id) || 0,
    }));



    // console.log(mergedArray, 'merge array')

    if (mergedArray.length !== 0) {
      res.status(201).send(mergedArray); 
    } else {
      if (searchTerm === "") {
        res.status(404).send({ msg: "no user found in conversation" });
      } else {
        // const globalSearch = await userSchema.find({
        //   username: { $regex: searchTerm, $options: "i" },
        // });
       
        const globalSearch = await userSchema.find({
          $or: [
            { username: { $regex: searchTerm, $options: "i" } },
            { name: { $regex: searchTerm, $options: "i" } }
          ]
        });
        

        let names = [];

        await Promise.all( 
          globalSearch.map(async (item) => {
            const { profilePic, isVerified } = await profileSchema.findOne({ Id: item._id });

            const conversation = await conversationSchema.findOne({
              conversationFor: item._id,
            });

            names.push({
              profile: profilePic,
              name: item.name,
              verified: isVerified,
              Id: item._id,
              text: "you are not connected",
              // date: getFormattedDate(date),
            });

            // if (conversation && conversation.members) {
            //   names = names.filter((user) => user.Id === userId);
            // }


         names = names.filter((user) => user.Id.toString() !== userId  );
        

          })
        );


        console.log(names)


        if (names.length === 0) {
          res.status(404).send({ msg: "no user found" });
        } else {
          res.status(201).send(names);
        }
      }
    }

    // console.log(globalSearch)
  } catch (error) {
    console.log(error);
  }
};

export default searchController;
