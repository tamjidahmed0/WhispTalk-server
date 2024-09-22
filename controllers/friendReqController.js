import friendReqSchema from '../models/friends.js'

const friendReqController = async(req, res)=>{
    const {userId, friendId} = req.body
    console.log(userId, friendId)
    try {
        const addFriend = await new friendReqSchema({userId, friendId}).save()
        console.log(addFriend)
    } catch (error) {
        console.log(error)
    }
}


export default friendReqController