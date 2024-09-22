import followingSchema  from '../models/following.js'
import followerSchema from '../models/followers.js'
import userSchema from '../models/user.js'

const followingController = async(req, res) =>{
    const {id} = req.query
    try {
      const following = await followingSchema.find({id})
      // const userDetails = await userSchema.find({id})

 
      let user = []
      

      await Promise.all(
        following.map(async (item) => {

          const userDetails = await userSchema.findOne(item.following)
          console.log(userDetails.name)


          user.push({followingId:item.following, name:userDetails.name})

      
        })
      );
  
      console.log(user);
      res.status(200).send(user);
    } catch (error) {
        
    }
}

export default followingController