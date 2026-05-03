import Redis from "ioredis"


function redisConnection() {
    return new Redis({
        host: "localhost",
        port: 6380
    })
}

const publisher = redisConnection();
const subscriber = redisConnection()
const redis = redisConnection()

export {
    redis,
    publisher,
    subscriber
}