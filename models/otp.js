import mongoose from "mongoose"
import bcrypt from 'bcryptjs'
const schema = mongoose.Schema

const userOtp = new schema({
    name:{
        type:String,
        require:true
    },
    username:{
        type:String,
        require:true,
        // unique:true
    },
    email:{
        type:String,
        
    },
    password:{
        type:String,
        require:true
    },
    date:{
        type:Date,
        // default:Date.now
        // default: new Date().getTime() + 3 * 60000 
        default: () => new Date(new Date().getTime() + 3 * 60000), 
    },
    otp:{
        type:String  
    },
    token:{ 
        type:String
    },
    // delay:{
    //     type:Number  
    // }  

    // createdAt: { type: Date, default: Date.now, index:{expireAfterSeconds:10}} 
    create:{
        type:Date,
        default:Date.now
    }

    

})


// userOtp.index({createdOn: 1}, {expireAfterSeconds: 60 * 6})


userOtp.pre('save', async function (next){
    const user = this
    
    if(user.isModified('password')){
    const salt = await bcrypt.genSalt(10)
    user.password = await bcrypt.hash(user.password,salt)
    user.otp = await bcrypt.hash(user.otp, salt)

    next()
    }
    })




    userOtp.pre('findOneAndUpdate', async function (next) {
        const user = this._update; // Access the update object
        
        // Check if password or otp is modified in update operation
        if (user.password || user.otp) {
            const salt = await bcrypt.genSalt(10);
            if (user.password) {
                user.password = await bcrypt.hash(user.password, salt);
            }
            if (user.otp) {
                user.otp = await bcrypt.hash(user.otp.toString(), salt.toString());

            }
        }
    
        next();
    });


const collection = mongoose.model('otp', userOtp)

export default collection