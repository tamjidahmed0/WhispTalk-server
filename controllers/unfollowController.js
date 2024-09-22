import followingSchema from '../models/following.js'
import followerSchema from '../models/followers.js'
import userSchema from '../models/user.js'

const unfollowController = async ( req, res) =>{
    const {request, id} = req.query
    try {
    const find = await followingSchema.findOneAndDelete({id: req.query.id, following:req.query.request})
    const find2 = await followerSchema.findOneAndDelete({id:req.query.request, followers: req.query.id})

    const name = await userSchema.findById(req.query.request)


    if(find && find2){
      res.status(201).send({msg: `you unfollowed ${name.name}`})
    }


    } catch (error) {
        console.log(error)
    }
}

export default unfollowController