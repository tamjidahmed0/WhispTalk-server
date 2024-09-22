import userSchema from '../models/user.js'
import profileSchema from '../models/profile.js'


const updateController = async (req ,res) =>{
    const {name, profile} = req.body
    const username = req.body.username.toLowerCase()
    const {userId} = req.params
    const usernameRegex = /^[a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*$/;
    const NamePattern = /^\s*\S+(?:\s+\S+)*\s*$/;
    const whiteSpace = /^\s*$/
    
    try {
        
        if(whiteSpace.test(name && username)){
            res.status(400).send({msg:'Please enter name or username', status:400})
        }else{
            const user = await userSchema.findOne({_id: userId})

      
            if(user !== null){
    
    if((username && name) === '' || !NamePattern.test(name)){
        res.status(400).send({msg:'Inavlid formate', status:400})
    }else{
        if(!usernameRegex.test(username)){
            res.status(400).send({msg:'Username must contain only letters, numbers.', status:400})
        }else{
        
            const isUserExist = await userSchema.findOne({username: username})
            console.log(isUserExist, 'line28')
        
            if(isUserExist !== null){
        
                if(user.username === username){
                    console.log('current username')
                    res.status(400).send({msg:'It is your current username' , status:400})
                    }else{
                        res.status(400).send({msg:'Username already exist' , status:400})
                    }
        
        
                
            }else{
        
        
        
        
                if(Object.keys(username).length < 5 ){
                    res.status(400).send({msg:'Usernames must be at least 5 characters long.' , status:400})
            
                }else{
                    if(username.startsWith('_') || username.endsWith('_')){
                        res.status(400).send({msg:'Username cannot start or end with an underscore.' , status:400})
                    }else{
            
                      
            
                        const userUpdate = await userSchema.findByIdAndUpdate(userId, {
                            $set:{
                                name : name !== '' ? name : undefined,
                                username: username !== '' ? username : undefined           
                            }
                        }, {new:true})
            
                        console.log('else work')
                       
                        if(userUpdate){
                            console.log(userUpdate)
                            res.status(201).send({name: userUpdate.name , username: userUpdate.username,  status:201})
                        }
            
            
                    }
                    
                }
        
        
        
        
        
        
            }
        
        
        
        
        
            
        }
    }
    
    
    
    
    
    
    
    
    
    
    
            }
    
    
    
    
    
    
        }

 

    } catch (error) {
        console.log(error)
    }
}

export default updateController