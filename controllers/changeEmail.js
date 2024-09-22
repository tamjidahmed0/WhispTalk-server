import userSchema from '../models/user.js'
import * as dotenv from 'dotenv' 
dotenv.config()



const changeEmail = async(req , res) =>{
const userId = req.params.userId
const {currentEmail, newEmail } = req.body

    try {

        if(currentEmail === newEmail){
            res.status(409).send({msg: 'Your current and new emails are same'})
        }else{
            
            const is_new_email_exist = await userSchema.findOne({email:newEmail})

            if(is_new_email_exist){
                res.status(409).send({msg: "You can't use this email"})
            }else{
                const findEmail = await userSchema.findById(userId)
                if(findEmail){
                  const update_mail = await userSchema.findByIdAndUpdate(userId, {email:newEmail}, {new:true})
                    if(update_mail){
                        res.status(201).send({msg: 'Email change successfully', email: update_mail.email, status:201})
                    }
                  
    
                }
            }


          
        }

      
    } catch (error) {
        console.log(error)
    }
 


}

 
export default changeEmail