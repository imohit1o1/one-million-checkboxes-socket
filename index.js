import http from 'node:http';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

import express from 'express';
import { Server } from 'socket.io';
import { CLIENT_EVENTS, SERVER_EVENTS } from './constants.js';
import { CHECKBOXES_SIZE } from './public/constants.js';
import { publisher, subscriber, redis } from "./redis.js";
import { channel } from 'node:diagnostics_channel';


const CHECKBOX_STATE_KEY = 'checkbox-state'

const rateLimitHashMap = new Map();


async function main() {
    const PORT = process.env.PORT || 8002;
    const app = express();
    const server = http.createServer(app);

    const io = new Server(server);


    await subscriber.subscribe(`internal-server:${SERVER_EVENTS.CHECKBOX_UPDATED}`);

    subscriber.on('message', (channelName, message) => {
        if (channelName === `internal-server:${SERVER_EVENTS.CHECKBOX_UPDATED}`) {
            const { index, checked } = JSON.parse(message);
            io.emit(SERVER_EVENTS.CHECKBOX_UPDATED, { index, checked });
        }
    })

    io.on('connection', (socket) => {
        console.log(`Socket connected: ${socket.id}`);

        // socket.on means listening the event
        // listen the toggle checkbox event from the client
        socket.on(CLIENT_EVENTS.TOGGLE_CHECKBOX, async (data) => {
            console.log(`Socket:[${socket.id}:${CLIENT_EVENTS.TOGGLE_CHECKBOX}]`, data);

            // after listening we have to send this data to all sockets except itself
            // io means everyone
            // server send the event and data
            //! after emitting the event to all sockets we have to update the initial state so new socket get the old data
            // io.emit(SERVER_EVENTS.CHECKBOX_UPDATED, data);
            // initialState.checkboxes[index] = checked;


            const lastOperationTime = rateLimitHashMap.get(socket.id);
            const now = Date.now();

            if (lastOperationTime) {
                const timeElapsed = now - lastOperationTime;
                if (timeElapsed < 1000) { // 1 second rate limit for better UX testing
                    socket.emit(SERVER_EVENTS.RATE_LIMIT, { message: "Slow down! You're clicking too fast." });
                    return;
                }
            }
            rateLimitHashMap.set(socket.id, now);

            const existingState = await redis.get(CHECKBOX_STATE_KEY);
            let state;
            if (existingState) {
                state = JSON.parse(existingState);
            } else {
                state = new Array(CHECKBOXES_SIZE).fill(false);
            }

            state[data.index] = data.checked;
            await redis.set(CHECKBOX_STATE_KEY, JSON.stringify(state));

            //* instead to emit directly to all cients we send that event and data to the redis sever
            //* client -> server -> redis -> server(emit to their client and save state or data)
            await publisher.publish(
                `internal-server:${SERVER_EVENTS.CHECKBOX_UPDATED}`,
                JSON.stringify(data)
            )


        })
    });

    app.use(express.static(path.resolve("./public")));

    app.get('/health', (req, res) => {
        res.status(200).json({
            message: 'server is healthy'
        });
    });


    app.get('/checkboxes', async (req, res) => {
        const existingState = await redis.get(CHECKBOX_STATE_KEY);
        if (existingState) {
            const remoteData = JSON.parse(existingState);
            return res.json({ checkboxes: remoteData })
        }
        else {
            return res.json({ checkboxes: new Array(CHECKBOXES_SIZE).fill(false) })
        }
    })
    server.listen(PORT, () => {
        console.log(`Server is listening on http://localhost:${PORT}`);
    });
}

main();
