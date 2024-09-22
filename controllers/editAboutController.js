import profileSchema from '../models/profile.js'
const EditAbout = async ( req, res) =>{
    const userId = req.params.userId
    const {about} = req.body
    try {
 
        if(about.length > 101){
            res.status(400).send({msg: 'Max lenght', status:400})
        }else{
            const editAbout = await profileSchema.findOneAndUpdate({Id: userId}, {about:about}, {new:true})
            res.status(201).send({about: editAbout.about, msg: 'About updated successfully!', status:201})
        }

        
        
    } catch (error) {
        console.log(error)
    }
}

export default EditAbout