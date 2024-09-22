
import connectionReqSchema from '../models/connectionRequest.js'
import profileSchema from '../models/profile.js'
const getConnectionRequest = async (req, res) =>{
    const {userId} = req.params
    try {
        const connection = await connectionReqSchema.find({requestReceiveId:userId})
        const count = await connectionReqSchema.countDocuments({requestReceiveId:userId})

        let data = []

       await Promise.all(
        connection.map(async(item)=>{
           const profile = await profileSchema.findOne({Id:item.requestSentId})
           if(profile){
                data.push({Id: profile.Id, name: profile.name, picture: profile.profilePic})
           }
        })
       )
       if(data.length === 0){
        res.status(404).send({msg: 'You have 0 connection request'})
       }else{
        res.status(201).send({count: count, data: data})
       
       }

    
      
    } catch (error) {
        console.log(error)
    }
}

export default getConnectionRequest