// Assuming you have TensorFlow.js installed and set up in your project
// import * as tf from '@tensorflow/tfjs-node';
import * as fs from 'fs';
import _ from 'lodash'
import recommendationSchema from '../models/recommendation.js'
import userSchema from '../models/user.js'


const recommendation = async ()=>{
// Function to check if a directory exists
// const directoryExists = (directory) => {
//     try {
//       return fs.statSync(directory).isDirectory();
//     } catch (err) {
//       return false;
//     }
//   }; 
  

//   const weights = {
//     likes: 0.5,
//     retweets: 1.0,
//     comments: 13.5,
//     profileEngagement: 12.0,
//     videoWatch: 0.005,
//     replyEngagement: 75.0,
//     conversationEngagement: 11.0,
//     conversationDuration: 10.0,
//     negativeReaction: -74.0,
//     report: -369.0
//   };
  
  
   
  
//   // Example user data
//   const usersData = [
//     { username: 'tamjid', likes: 1 * weights.likes, comments: 1 * weights.comments, shares: 1, report: 0 * weights.report, relevanceScore: 0.666 },
//     { username: 'shofik', likes: 50 * weights.likes, comments: 10 * weights.comments, shares: 10, report: 0 * weights.report, relevanceScore: 1 },
//     { username: 'mithila', likes: 52 * weights.likes, comments: 10 * weights.comments, shares: 20, report: 0 * weights.report, relevanceScore: -0.6 },
//     { username: 'raju', likes: 520 * weights.likes, comments: 180 * weights.comments, shares: 20, report: 0 * weights.report, relevanceScore: -360.0 },
//     { username: 'sumit', likes: 150 * weights.likes, comments: 10 * weights.comments, shares: 0, report: 1 * weights.report, relevanceScore: 1 },
  
  
//     // Add more user data as needed
//   ];
  
//   // Sort users by predicted relevance score in descending order
//   usersData.sort((a, b) => b.relevanceScore - a.relevanceScore);
  
//   // Define relevance score threshold (e.g., 0.5)
//   const relevanceThreshold = 0.1;
  
  
//   // Function to generate recommendations for each user
//   const generateRecommendations = (usersData) => {
//     for (const user of usersData) {
//       const recommendedUsers = usersData.filter(u => u.username !== user.username && u.relevanceScore > relevanceThreshold);
//       console.log(`Recommendations for ${user.username}:`, recommendedUsers);
//     } 
//   };
  
//   // Generate initial recommendations
//   // generateRecommendations(usersData);
  
  
  
//   // Function to train the model
//   // const trainModel = async () => {
//   //   // Define a TensorFlow.js model for generating recommendations
//   //   const model = tf.sequential();
//   //   model.add(tf.layers.dense({ units: 128, activation: 'relu', inputShape: [3] })); // Increased units
//   //   model.add(tf.layers.dense({ units: 64, activation: 'relu' })); // Added an additional dense layer
//   //   model.add(tf.layers.dense({ units: 1 }));
  
//   //   // Compile the model
//   //   model.compile({ optimizer: 'adam', loss: 'meanSquaredError' });
  
//   //   // Train the model with user data
//   //   const features = tf.tensor2d(usersData.map(user => [user.likes, user.comments, user.shares]));
//   //   const labels = tf.tensor2d(usersData.map(user => [user.relevanceScore])); // Assuming you have relevance scores for users
//   //   await model.fit(features, labels, { epochs: 1000, batch_size: 4 }); // Adjusted batch size
  
//   //   // Save the trained model
//   //   if (!directoryExists('trained_model')) {
//   //     fs.mkdirSync('trained_model');
//   //   } 
//   //   await model.save('file://trained_model');
//   // };
  
  
  
//   // Function to train the model
//   const trainModel = async () => {
//     // Define a TensorFlow.js model for generating recommendations
//     const model = tf.sequential();
//     model.add(tf.layers.dense({ units: 128, activation: 'relu', inputShape: [3], kernelInitializer: 'randomNormal' })); // Increased units
//     model.add(tf.layers.dense({ units: 64, activation: 'relu', kernelInitializer: 'randomNormal' })); // Added an additional dense layer
//     model.add(tf.layers.dense({ units: 1, kernelInitializer: 'randomNormal' }));
  
//     // Compile the model
//     model.compile({ optimizer: 'adam', loss: 'meanSquaredError' });
  
//     // Train the model with user data
//     const features = tf.tensor2d(usersData.map(user => [user.likes, user.comments, user.shares]));
//     const labels = tf.tensor2d(usersData.map(user => [user.relevanceScore])); // Assuming you have relevance scores for users
//     await model.fit(features, labels, { epochs: 1000, batch_size: 8 }); // Adjusted batch size
  
//     // Save the trained model
//     // if (!directoryExists('trained_model')) {
//     //   fs.mkdirSync('trained_model');
//     // }  
//     // await model.save('file://trained_model');
//   };
  
  
  
  
  
  
  
  
  
//   // Function to update the existing model with new data
//   // const updateModel = async () => {
//   //   // Load the existing model
//   //   const model = await loadModel();
  
//   //   // Train the loaded model with the updated dataset or new data
//   //   const updatedFeatures = tf.tensor2d(usersData.map(user => [user.likes, user.comments, user.shares]));
//   //   const updatedLabels = tf.tensor2d(usersData.map(user => [user.relevanceScore]));
//   //   await model.fit(updatedFeatures, updatedLabels, { epochs: 1000, batch_size: 4 });
  
//   //   // Save the updated model
//   //   await model.save('file://trained_model');
//   // };
  
  
  
//   // Function to load the model
//   const loadModel = async () => {
//     if (directoryExists('trained_model')) {
//       return await tf.loadLayersModel('file://trained_model/model.json');
//     } else {
//       console.log('No trained model found. Training model for the first time...');
//       await trainModel();
//       return await tf.loadLayersModel('file://trained_model/model.json');
//     }
//   };
  
  
  
  
//   // Make predictions for new users
//   const predictForNewUsers = async () => {
//     // Load the model

// await trainModel()

//     const model = await loadModel();
  
  
//     if (model) {
//       // Make predictions based on the trained model
//       const predictedRelevanceScores = await model.predict(tf.tensor2d(usersData.map(user => [user.likes, user.comments, user.shares])));
  
//       // Update relevance scores in usersData
//       usersData.forEach((user, index) => {
//         user.relevanceScore = predictedRelevanceScores.arraySync()[index][0];
//       });
  
//       // Sort users by the updated relevance score
//       usersData.sort((a, b) => b.relevanceScore - a.relevanceScore);
  
//       // Generate updated recommendations
//       generateRecommendations(usersData);
//     }
  
  
    
  
//     // Make predictions for new users
//     const newUsersData = [
//       { username: 'newUser1', likes: 120, comments: 60, shares: 25 },
//       { username: 'newUser2', likes: 800, comments: 290, shares: 100 },
//       { username: 'newUser2', likes: 80, comments: 20, shares: 100 },
//       { username: 'newUser2', likes: 80, comments: 20, shares: 100 },
//       // Add more new user data as needed 
//     ];
  
//     for (const newUser of newUsersData) {
//       const newUserData = tf.tensor2d([[newUser.likes, newUser.comments, newUser.shares, ]]);
//       const prediction = (await model.predict(newUserData).data())[0];
  
//       console.log(`Predicted relevance score for ${newUser.username}: ${prediction}`);
//     }
//   };
  
//   // Predict relevance scores for new users
//   predictForNewUsers();


















// import fs from 'fs'
// import _ from 'lodash'



// Create Mock User
const mockUser = {
    userId:"",
    category: [ 2 ],
};

// Create Weights
const weights = {
    watchPercentage: 10,
    ignored: 80,
    liked: 10,
    disliked: -50,
    shared: 10,
    subscribed: 310,
    skipped: -30
}

         
// const weights = {
//     watchPercentage: Math.floor(Math.random() * 100) + 1,
//     ignored: Math.floor(Math.random() * 100) + 1,
//     liked: Math.floor(Math.random() * 100) + 1,
//     disliked: Math.floor(Math.random() * 100) + 1,
//     shared: Math.floor(Math.random() * 100) + 1,
//     subscribed: Math.floor(Math.random() * 1000) + 1,
//     skipped: Math.floor(Math.random() * 100) + 1
// };



// Filter results 

const filterViewerPersonalization = (category) => { 
  return new Promise(async(resolve, reject) => {

    const scoreFind = await recommendationSchema.find()

// console.log(scoreFind)

    

          try {
              // Parse the JSON data
            //   let allViewerPersonalization = JSON.parse(data);
            //   console.log(allViewerPersonalization)
             
              // Filter the data based on the category
            //   let filteredData = allViewerPersonalization.filter((viewerPersonalization) => category.includes(viewerPersonalization.category));
              let filteredData = scoreFind.filter((viewerPersonalization) => category.includes(viewerPersonalization.score));
            //   resolve(filteredData);
              resolve(filteredData); 
          } catch (error) {
              // Handle JSON parsing error
              console.error("Error parsing JSON:", error);
              reject(error);
          }
   
  });
};


// Get 10 results

const getTopTenVideos = (data) => {
   
    return _.chain(data)
    .map((item) => [item.name,  calculateWeight(item), item.username, item.profilePic])
    .groupBy((item) => item[0])
    .map((items) => {
        
        const totalWeights = _.sumBy(items, (item) => item[1]);
        const avgWeight = totalWeights / items.length;

//   console.log(avgWeight)
        return {name: items[0][0], username:items[0][2], profile: items[0][3], score: avgWeight,}
    })
    .sortBy('score')
    .reverse()
    .take(10)
    .value()
}

// Calculation weights 

const maxWeight = Math.max(...Object.values(weights));

// const calculateWeight = (userData) => {

    
 
//     const { watchPercentage, ignored, liked, disliked, shared, subscribed, skipped } = userData;
//     const weight = (watchPercentage > 0) ? (weights.watchPercentage / watchPercentage) : 0 +
//         (ignored ? weights.ignored / maxWeight : 0) + (liked ? weights.liked / maxWeight : 0) +
//         (disliked ? weights.disliked / maxWeight : 0) + (shared ? weights.shared / maxWeight : 0) +
//         (subscribed ? weights.subscribed / maxWeight : 0) + (skipped ? weights.skipped / maxWeight : 0);
//     return (weight / (weights.watchPercentage + weights.ignored + weights.liked + weights.disliked + weights.shared + weights.subscribed + weights.skipped)) * 100;
// };



const calculateWeight = (userData) => {
    // console.log(userData)
    const { score } = userData;
    const weight = (weights.watchPercentage / score) +
        (weights.ignored / maxWeight) +
        (weights.liked / maxWeight) +
        (weights.disliked / maxWeight) +
        (weights.shared / maxWeight) +
        (weights.subscribed / maxWeight) +
        (weights.skipped / maxWeight);
    return (weight / (weights.watchPercentage + weights.ignored + weights.liked + weights.disliked + weights.shared + weights.subscribed + weights.skipped)) * 100;
};





// init() 
  
const init = async () => {
    const filteredData = await filterViewerPersonalization(mockUser.category);
    // console.log(getTopTenVideos(filteredData));
    return getTopTenVideos(filteredData)
}

// init();


return init()


   
 }

export default recommendation 