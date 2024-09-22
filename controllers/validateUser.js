
const validateUser = async (req, res) =>{
    try {
        res.status(201).send({isValidate: true})
    } catch (error) {
        
    }
}

export default validateUser