import express, { Request, Response } from 'express'
import { QueueMessage } from './model/QueueMessage';
import axios from 'axios';

const app = express();

app.use(express.json());

const PORT = 8000;
const DEFAULT_CONSUMER_URL = "http://localhost:8080/tomeagents/events/topic"
const CONSUMER_URL_MAP: { [key: string]: string } = {
    task: "http://localhost:8081/galebroker/events/agent",
}

const queue: QueueMessage[] = [];

app.post('/msg', (req: Request, res: Response) => {

    const msgId = `msg-${Date.now()}-${Math.random()}`;

    console.log(`Message ${msgId} received with body ${JSON.stringify(req.body)}`);

    const authHeader = req.get('authorization');

    console.log(authHeader);
    

    if (!authHeader) {
        res.status(403).json({ error: 'Missing Authorization header' });
        return;
    }

    queue.push({
        id: msgId,
        payload: req.body,
        attempts: 0,
        authHeader: authHeader,
    });

    console.log(`Message ${msgId} queued. Queue length: ${queue.length}`);

    res.status(200).json({ message: `Message ${msgId} queued successfully` });

    setImmediate(processQueue);

});

async function processQueue(): Promise<void> {

    // Process all available messages in parallel
    while (queue.length > 0) {
        const msg = queue.shift()!;

        // Process this message without blocking others
        processMessage(msg).catch(err => {
            console.error("Unexpected error in processMessage:", err);
        });
    }
}

async function processMessage(msg: QueueMessage): Promise<void> {
    try {

        const msgType = msg.payload.type;

        // Find the consumer URL 
        let consumerURL = DEFAULT_CONSUMER_URL;
        if (CONSUMER_URL_MAP[msgType]) consumerURL = CONSUMER_URL_MAP[msgType];

        console.log(`Delivering message ${msg.id} to endpoint ${consumerURL}`);

        await axios.post(consumerURL, msg.payload, {
            timeout: 600000,
            headers: {
                Authorization: msg.authHeader
            }
        });

        console.log(`Message ${msg.id} delivered successfully`);

    } catch (err: any) {

        console.error(`Delivery failed for message ${msg.id}:`, err.message);

        msg.attempts = (msg.attempts ?? 0) + 1;

        if (msg.attempts < 3) {

            console.log("Retrying later...");

            queue.push(msg);

            // Retry after delay
            setTimeout(processQueue, 1000);

        }
        else {
            console.error(`Dropping message ${msg.id} after 3 failed attempts`);
        }
    }
}

app.listen(PORT, () => {
    console.log(`Queue service running on port ${PORT}`);
});