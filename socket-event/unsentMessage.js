import messageSchema from '../models/message.js'
import profileSchema from '../models/profile.js'
import redisClient from '../redis-client/redisClient.js'


const unsentMessage = ({ io, socket, ONLINE_USERS_KEY }) => {

    socket.on('senderUnsent', async (data) => {
        console.log(data)

        async function isUserOnline(userId) {
            const _userId = await redisClient.hget(ONLINE_USERS_KEY, userId);
            return _userId;
          }


        const sender_find_message = await messageSchema.findOne({
            $and: [{ Id: data.messageId }, { textFor: data.receiverId }]
        })

        const receiver_find_message = await messageSchema.findOne({
            $and: [{ Id: data.messageId }, { textFor: data.userId }]
        }) 

    //    console.log((receiver_find_message !== null) &&  sender_find_message)

    if(sender_find_message.unsent !== null || receiver_find_message.unsent !== null){


        if (sender_find_message.unsent && receiver_find_message.unsent ) {

            console.log('its true')

            const delete_unsent = await messageSchema.findOneAndDelete({
                $and: [{ Id: data.messageId }, { textFor: data.userId }]
            })

            if (delete_unsent) {
                console.log('deleted')
            }

        } else {



            // console.log(find_message, 'find message')
            if (sender_find_message && receiver_find_message) {
                const get_profile_sender = await profileSchema.findOne({ Id: sender_find_message.senderId })



                const unsent_the_message_sender = await messageSchema.findOneAndUpdate({
                    $and: [{ Id: data.messageId }, { textFor: data.receiverId }]
                }, { text: `${get_profile_sender.name} unsent a message`, unsent: true }, { new: true })

                const unsent_the_message_receiver = await messageSchema.findOneAndUpdate({
                    $and: [{ Id: data.messageId }, { textFor: data.userId }]
                }, { text: `You unsent a message`, unsent: true }, { new: true })




                const check_online_user = await isUserOnline(data.receiverId)
                const check_online_user_sender = await isUserOnline(data.userId)


               console.log(unsent_the_message_sender.text)
 
                io.to(check_online_user).emit('senderUnsent',{
                    messageId: unsent_the_message_sender.Id,
                    name: get_profile_sender.name,
                    profile: get_profile_sender.profilePic,
                    iSend: data.userId,
                    whoSend: data.receiverId,
                    socketId: data.socket,
                    text: unsent_the_message_sender.text,
                    unsent: true,
                    type: 'text',
                    // Date: Dates,
                })







                io.to(check_online_user_sender).emit('_senderUnsent',{
                    messageId: unsent_the_message_receiver.Id,
                    name: get_profile_sender.name,
                    profile: get_profile_sender.profilePic,
                    iSend: data.userId,
                    whoSend: data.receiverId,
                    socketId: data.socket,
                    text: unsent_the_message_sender.text,
                    unsent: true,
                    type: 'text',





                    messageId: unsent_the_message_receiver.Id,
                    iSend: data.userId,
                    text: unsent_the_message_receiver.text,
                    // Date: Dates,
                    unsent: true,
                    receiverId: data.receiverId,
                    messageStatus: "sent",
                    
                })


            }


      





        }
    }

      


    })



}

export default unsentMessage 