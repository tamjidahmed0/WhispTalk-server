import connectionRequestSchema from '../models/connectionRequest.js'
import profileSchema from '../models/profile.js'
const connectionRequest = async (req, res) =>{
    const {userId, requestId} = req.params
    try {
        // 

        const check = await connectionRequestSchema.findOne({
            requestSentId:userId,
            requestReceiveId: requestId,
            requestSentId:requestId,
            requestReceiveId: userId,
             
        })

    

if(check === null){
    const addRequest = await new connectionRequestSchema({
        requestSentId: userId,
        requestReceiveId: requestId,
     
    }).save()

    if(addRequest){
        const {name} = await profileSchema.findOne({Id:addRequest.requestReceiveId})
            res.status(201).send({msg :`Request sent to ${name}`})
    }

   
}else{
    console.log(check)
}

 
  
    } catch (error) {
        console.log(error)  
    }
}

export default connectionRequest   