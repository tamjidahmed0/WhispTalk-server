import followingSchema from '../models/following.js'
import followerSchema from '../models/followers.js'

const countConnectionController = async ( req, res) =>{
    const {id} = req.query
    try {
        const following = await followingSchema.countDocuments({id:id })
        const follower = await followerSchema.countDocuments({id:id })

        res.status(201).send({following, follower})
    } catch (error) {
        
    }
}

export default countConnectionController