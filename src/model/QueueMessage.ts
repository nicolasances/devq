export interface QueueMessage {
  id: string;
  payload: any;
  attempts: number;
  authHeader: string;
}
