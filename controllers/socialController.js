import followingSchema from '../models/following.js'
import followerSchema from '../models/followers.js'
import userSchema from '../models/user.js'


const socialController = async(req, res) =>{
   const { request , id} = req.query

    console.log(req.query)

    try {

      const find = await followingSchema.findOne({id: req.query.id, following:req.query.request})
      const find2 = await followingSchema.findOne({id:req.query.request, followers: req.query.id})

      const name = await userSchema.findById(req.query.request)
      

if(request !== id){
   if(!find && !find2){
      const insert = new followingSchema ({
         id: req.query.id,
         following:req.query.request
        }).save()
 
 
        const insert2 = new followerSchema ({
         id:req.query.request,
         followers: req.query.id
        }).save()
 
        res.status(201).send({msg: `You following ${name.name}`})
 
       }else{
         res.status(201).send({msg: `You following ${name.name}`})
       }
}else{
   res.status(400).send({msg:'something wrong!'})
}




       
    } catch (error) {
       console.log(error) 
    }

}

export default socialController