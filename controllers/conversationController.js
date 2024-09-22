import conversationSchema from "../models/conversation.js";
import profileSchema from "../models/profile.js";
import moment from "moment";
import mongoose from "mongoose";
import unReadSchema from "../models/unReadMsg.js";


const conversations = async (req, res) => {
  const userId =  req.params.userId;
  // const userId = mongoose.Types.ObjectId(req.params.userId)
  // const userId = req.params.userId;
console.log(userId , 'user id')
  try {
    //find conversation in conversationSchema
    const conversation = await conversationSchema.find({ 
      conversationFor:userId,
    });

    const unread = await unReadSchema.find({receiverId : userId})

    const countsMap = new Map(unread.map(({ senderId, count }) => [senderId.toString(), count]));

  


    // console.log(countsMap, 'countsmap')

    //store data in array
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

      const   profilesdd = await profileSchema.findOne({
          Id: item.members[1],
        });

       

        const profilePic = profiles ? profiles.profilePic : "";
        // console.log(profiless)

        names.push({
          profile: profilePic,
          verified: item.isVerified,
          name: item.receiverName,
          Id: item.members[1],
          convText: item.text,
          date: getFormattedDate(date),
        });
      })
    );


 
    const mergedArray = names.map(user => ({
      ...user,
     unReadMsgCount: countsMap.get(user.Id) || 0
      
    }));

    // console.log(mergedArray)
    // console.log(names);
    res.status(200).send(mergedArray);
  } catch (error) {
    res.status(500).json(error);
    console.log(error)
  }
};

export default conversations;
