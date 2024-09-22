/**
 * user: {
 *    id: string;
 *    name: string;
 * }
*/




import fs from 'fs';
// import { v4 as uuidv4 } from 'uuid';
import { v4 as uuidv4 } from 'uuid';







const users = [];

function generateUsers() {
    for (let i = 0; i < 50; i++) {
        users.push({
            id: uuidv4(),
            name: `User ${i}`,
        });
        if (i === 49) {
            fs.writeFileSync('data/users.json', JSON.stringify(users));
        }
    }
}

// generateUsers();


/**
category: {
 *    id: string;
 *    name: string;
}
*/

const category = [];

function generateCategories() {
    for (let i = 0; i < 10; i++) {
        category.push({
            id: uuidv4(),
            name: `Category ${i}`,
        });
        if (i === 9) {
            fs.writeFileSync('data/category.json', JSON.stringify(category));
        }
    }
}

generateCategories();

/**
channel: {
 *    id: string;
 *    name: string;
 *    category: category;
}
*/

const channel = [];

function generateChannel() {
    for (let i = 0; i < 20; i++) {
        channel.push({
            id: uuidv4(),
            name: `Channel ${i}`,
            category: category[Math.random() * category.length | 0].id,
        });
        if (i === 19) {
            fs.writeFileSync('data/channel.json', JSON.stringify(channel));
        }
    }
}

generateChannel();

/**
 * video: {
 *    id: string;
 *    title: string;
 *    category: category;
 *    duration: string;
 *    channel: string;
 * }
 */

const video = [];

function generateVideo() {
    for (let i = 0; i < 2000; i++) {
        video.push({
            id: uuidv4(),
            title: `Video ${i}`,
            category: category[Math.random() * category.length | 0].id,
            duration: `${Math.random() * 1000 | 0}`,
            channel: channel[Math.random() * channel.length | 0].id,
        });
        if (i === 1999) {
            fs.writeFileSync('data/video.json', JSON.stringify(video));
        }
    }
}

generateVideo();

/**
 * viewerPersonalization: {
 *    id: string;
 *    userId: string;
 *    videoId: string;
 *    timestamp: number;
 *    watchLength: number;
 *    watchPercentage: number;
 *    ignored: boolean;
 *    liked: boolean;
 *    disliked: boolean;
 *    shared: boolean;
 *    subscribed: boolean;
 *    skipped: boolean;
 * }
*/

const viewerPersonalization = [];

function generateViewerPersonalization() {
    const data = fs.readFileSync('../badwords/badwords.txt', { encoding: 'utf8' });
    const badWords = data.split('\n').map(word => word.trim());
    for (const items of badWords) {
        console.log(items)
                viewerPersonalization.push({
              
                words:items
                })
             
        fs.writeFileSync('data/badword.json', JSON.stringify(viewerPersonalization));
        // for (let j = 0; j < 500; j++) {
  
        //     viewerPersonalization.push({
        //         id: uuidv4(),
                
          
        //     });  
        //     if (j === 499) {
        //         fs.writeFileSync('data/viewerPersonalization.json', JSON.stringify(viewerPersonalization));
        //     }
        // }
    }
}

generateViewerPersonalization()