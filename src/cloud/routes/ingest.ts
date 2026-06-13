import { Router } from 'express';
import { handleSessionSync } from '../ingest/handlers';

const router = Router();

// Existing routes would be here...

// New route for session sync
router.post('/session_sync', handleSessionSync);

export default router;