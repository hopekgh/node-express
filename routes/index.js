const express = require('express');
const mongoose = require('mongoose');
const redis = require('redis');

const app = express();
const PORT = 3000;

// MongoDB 연결
mongoose.connect("mongodb+srv://hopekgh:vuQquv-fukvu6-ruvnud@cluster0.mqzgbt4.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp", { useNewUrlParser: true, useUnifiedTopology: true });
const db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', () => {
  console.log('Connected to MongoDB');
});

// MongoDB 스키마 및 모델 정의
const schoolSchema = new mongoose.Schema({
  name: String,
  clicks: { type: Number, default: 0 }
});
const School = mongoose.model('School', schoolSchema);

// Redis 클라이언트 생성
const redisClient = redis.createClient();
redisClient.on('error', err => {
  console.log('Error:', err);
});

app.use(express.json());

// 클릭 수 증가 엔드포인트
app.post('/incrementClick/:schoolName', (req, res) => {
    const schoolName = req.params.schoolName;
    const incrementValue = req.body.clicks || 1;

    // 레디스에서 학교의 클릭수 증가
    client.incrby(schoolName, incrementValue, (err, reply) => {
        if (err) {
            return res.status(500).json({ error: 'Server Error' });
        }

        res.json({ message: 'Incremented successfully', clicks: reply });
    });
});

// 클릭 수를 MongoDB에 동기화하는 함수
const syncClickCountsToDb = async () => {
    redisClient.keys('*', (err, keys) => {
        if (err) throw err;

        keys.forEach(key => {
            redisClient.get(key, async (err, value) => {
                if (err) throw err;
                const clicksToAdd = Number(value);
                await School.updateOne({ name: key }, { $inc: { clicks: clicksToAdd } });
                redisClient.set(key, '0');
            });
        });
    });
      redisClient.del('school_ranking');
};

// 학교의 순위를 Redis에서 가져와서 반환하는 함수
const getSchoolRanking = async () => {
    const cachedRanking = await new Promise((resolve, reject) => {
        redisClient.get('school_ranking', (err, data) => {
            if (err) reject(err);
            resolve(data);
        });
    });

    if (cachedRanking) {
        return JSON.parse(cachedRanking);
    }

    const schools = await School.find().sort({ clicks: -1 }).limit(20).exec();
    const ranking = schools.map((school, index) => ({
        rank: index + 1,
        name: school.name,
        clicks: school.clicks
    }));

    // 10분 동안 캐시 저장
    redisClient.set('school_ranking', JSON.stringify(ranking), 'EX', 10 * 60);

    return ranking;
};

app.get('/ranking', async (req, res) => {
    try {
        const ranking = await getSchoolRanking();
        res.json(ranking);
    } catch (err) {
        res.status(500).json({ error: 'Server Error' });
    }
});

// 특정 학교의 순위 반환
app.get('/rank/:schoolName', async (req, res) => {
    const ranking = await getSchoolRanking();
    const schoolRank = ranking.find(school => school.name === req.params.schoolName);
    
    if (schoolRank) {
        res.json({ rank: schoolRank.rank });
    } else {
        res.json({ message: 'School not found in top 20' });
    }
});
// 10분마다 동기화
setInterval(syncClickCountsToDb, 10 * 60 * 1000);

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});
