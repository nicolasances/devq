export interface QueueMessage {
  payload: any;
  attempts: number;
  authHeader: string;
}
