import msgRequest from '../models/request.js'
import profileSchema from "../models/profile.js";
import moment from "moment";

const  messageReqController = async(req, res) =>{
    const userId =  req.params.userId;

try {
    const messageRequest = await msgRequest.find({
        conversationFor:userId,
      });
    const countrequest = await msgRequest.countDocuments({
        conversationFor:userId,
      });



      let names = [];

      await Promise.all(
        messageRequest.map(async (item) => {
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

          console.log(userId, 'jgjg')
  
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
            convText: item.text,
            date: getFormattedDate(date),
            
           
          });
        })
      );
  
      console.log(names); 
      res.status(200).send({msg: `You have ${countrequest} message request`,   count : countrequest ,  data: names});
 



  
} catch (error) {
    console.log(error)
}



}

export default messageReqController