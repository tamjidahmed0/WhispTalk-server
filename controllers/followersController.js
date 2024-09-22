import followingSchema  from '../models/following.js'
import followerSchema from '../models/followers.js'
import userSchema from '../models/user.js'
import profileSchema from '../models/profile.js'

const followersController = async (req, res) =>{
    const {id} = req.query
    try {
      const followers = await followerSchema.find({id})
      // const userDetails = await userSchema.find({id})

 
      let user = []
      

      await Promise.all(
        followers.map(async (item) => {

          const userDetails = await userSchema.findOne(item.followers)
          const userProfile = await profileSchema.findOne({Id :item.followers})
          console.log(userProfile)


          user.push({followerId:item.followers,profile:userProfile.profilePic , name:userDetails.name})

      
        })
      );
  
      console.log(user);
      res.status(200).send(user);
    } catch (error) {
        
    }
}

export default followersController