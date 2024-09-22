
import friendlistSchema  from '../models/friends.js'
import profileSchema from '../models/profile.js'


const friendlistController = async (req, res) =>{
    const userId = req.params.userId
    console.log(userId)
    try {
        const friendlist = await friendlistSchema.find(
          {
            $or:[
                {userId:userId},
                {friendId:userId}
            ]
          }
        )
   

        let array = []

        for(const item of friendlist){
console.log(item)

if(item.friendId.toString() === userId){
    const profile = await profileSchema.findOne({Id:item.userId})
           
    array.push({userId: profile.Id, profile : profile.profilePic, name: profile.name,})
}else {
        const profile = await profileSchema.findOne({Id:item.friendId})
           
            array.push({userId: profile.Id, profile : profile.profilePic, name: profile.name,})
}


            // const profile = await profileSchema.findOne({Id:item.friendId})
           
            // array.push({userId: profile.Id, profile : profile.profilePic, name: profile.name,})
        }

  res.status(201).send(array)
        
    } catch (error) {
        
    }
}

export default friendlistController