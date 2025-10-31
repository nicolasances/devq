import express, { Request, Response } from 'express'
import { QueueMessage } from './model/QueueMessage';
import axios from 'axios';

const app = express();

app.use(express.json());

const PORT = 8000;
const CONSUMER_URL = "http://localhost:8081/galebroker/events/agent"

const queue: QueueMessage[] = [];

app.post('/msg', (req: Request, res: Response) => {

    const msgId = `msg-${Date.now()}-${Math.random()}`;

    console.log(`Message ${msgId} received with body ${JSON.stringify(req.body)}`);

    const authHeader = req.get('authorization');

    if (!authHeader) {
        res.status(403).json({ error: 'Missing Authorization header' });
        return;
    }
    
    queue.push({
        payload: req.body,
        attempts: 0,
        authHeader: authHeader,
    });

    console.log(`Message ${msgId} queued. Queue length: ${queue.length}`);

    res.status(200).json({ message: 'Message queued successfully' });

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
        console.log(`Delivering message to endpoint ${CONSUMER_URL}`);
        
        await axios.post(CONSUMER_URL, msg.payload, {
            timeout: 600000,
            headers: {
                Authorization: msg.authHeader
            }
        });
        
        console.log("Delivered successfully");

    } catch (err: any) {

        console.error("Delivery failed:", err.message);

        msg.attempts = (msg.attempts ?? 0) + 1;

        if (msg.attempts < 3) {
            
            console.log("Retrying later...");
            
            queue.push(msg);
            
            // Retry after delay
            setTimeout(processQueue, 1000);

        } 
        else {
            console.error("Dropping message after 3 failed attempts");
        }
    }
}

app.listen(PORT, () => {
  console.log(`Queue service running on port ${PORT}`);
});