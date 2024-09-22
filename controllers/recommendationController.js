import recommendation from "../recommendation/recommendation.js";
import friendlistSchema from "../models/friends.js";
import userSchema from "../models/user.js";
import profileSchema from "../models/profile.js";


const recommendationController = async (req, res) => {
  const userId = req.params.userId;
  try {
    const alluser = await userSchema.find();

    let userArray = [];

    for (const item of alluser) {
      const id = await friendlistSchema.find({
        $or: [{ userId: item._id }, { friendId: item._id }],
      });

      for (const list of id) {
        if (item._id.toString() === list.userId.toString()) {
          const profile = await profileSchema.findOne({ Id: list.friendId });
          userArray.push({ Id: item._id, userId: profile.Id, name: item.name });
        } else {
          const profile = await profileSchema.findOne({ Id: list.userId });
          userArray.push({ Id: item._id, userId: profile.Id, name: item.name });
        }
      }
    }

    const transformedObject = {};

    userArray.forEach((user) => {
      const { userId, Id, ...userData } = user;
      if (!transformedObject[userId]) {
        transformedObject[userId] = { userId, friends: [] };
      }
      transformedObject[userId].friends.push({ Id, ...userData });
    });

    const finaldata = Object.values(transformedObject);

    const users = {};
    finaldata.forEach((user) => {
      //  console.log(user)
      users[user.userId.toString()] = user;
    });

    // Function to recommend new friends based on mutual connections and country
    function recommendFriends(currentUserId) {
      const currentUser = users[currentUserId];

      // currentUser.friends.map(friend => console.log(friend.userId))

      if (!currentUser) {
        console.log(`User ${currentUserId} not found.`);
        return [];
      }

      const currentUserFriends = new Set(currentUser.friends.map((friend) => friend.Id.toString()));
      const currentUserCountry = currentUser.country;

      let recommendedFriends = [];

      // Iterate over users
      for (let userId in users) {
        if (userId !== currentUserId) {
          const user = users[userId];

          //user.country === currentUserCountry &&
          // Check if the user is from the same country and not already a friend
          if (!currentUserFriends.has(userId)) {
            let mutualFriendsCount = 0;
            // Count mutual friends
            user.friends.forEach((friend) => {
              if (currentUserFriends.has(friend.Id.toString())) {
                mutualFriendsCount++;
              }
            });
            // Recommend user if they have at least one mutual friend
            if (mutualFriendsCount > 0) {
              recommendedFriends.push({ userId, mutualFriends: mutualFriendsCount });
            }
          }
        }
      }

      // Sort recommended friends by number of mutual friends
      recommendedFriends.sort((a, b) => b.mutualFriends - a.mutualFriends);

      return recommendedFriends;
    }

    const currentUserId = userId;
    const recommendedFriends = recommendFriends(currentUserId);
    // console.log(`Recommended friends for ${currentUserId}:`, recommendedFriends);

    let userDetails = [];

    for (const item of recommendedFriends) {
      const findUserDetails = await profileSchema.findOne({ Id: item.userId });
      userDetails.push({ userId: item.userId, profile: findUserDetails.profilePic, name: findUserDetails.name, mutual: item.mutualFriends, verified: findUserDetails.isVerified });
    }

    res.status(201).send(userDetails);
  } catch (error) {}
};

export default recommendationController;
