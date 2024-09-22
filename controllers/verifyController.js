import jwt  from 'jsonwebtoken'
const veryController = async (req , res) =>{

    let token

    const {authorization} = req.headers
    
    if(authorization ){
      try {
    
        token = authorization.split(' ')[1]
        const result = jwt.verify(token, process.env.JWT_SECRET)
    
        
    
       
        res.status(201).send({id: result.id, status:201})
     
    
      } catch (error) {
        res.status(401).send({msg: 'Invalid token', status:401})
      }
    
    }else{
      res.status(401).send({msg: 'please token provid' , status:401})
    }
    
    
    
      };
    
    
    
    



export default veryController