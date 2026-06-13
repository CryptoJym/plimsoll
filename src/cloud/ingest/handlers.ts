import { Request, Response } from 'express';
import { upsertSession } from '../db/sessions';
import { logger } from '../utils/logger';

/**
 * Handles the ingestion of session sync events.
 * This is triggered by the collector when a stitched session is finalized.
 */
export async function handleSessionSync(req: Request, res: Response): Promise<void> {
  try {
    const { kind, session_id, user_id, start_time, end_time, event_count, duration_ms } = req.body;

    // Validation
    if (kind !== 'session_sync') {
      res.status(400).json({ error: 'Invalid kind. Expected "session_sync".' });
      return;
    }

    if (!session_id || !user_id || !start_time || !end_time) {
      res.status(400).json({ error: 'Missing required fields: session_id, user_id, start_time, end_time' });
      return;
    }

    logger.info(`Processing session sync for session_id: ${session_id}, user_id: ${user_id}`);

    // Upsert the session into the workspace (Grow-only strategy)
    const result = await upsertSession({
      session_id,
      user_id,
      start_time,
      end_time,
      event_count,
      duration_ms,
    });

    if (result.isNew) {
      logger.info(`New session created: ${session_id}`);
    } else {
      logger.info(`Session updated (grow-only): ${session_id}`);
    }

    res.status(200).json({ 
      success: true, 
      session_id, 
      action: result.isNew ? 'created' : 'updated' 
    });

  } catch (error) {
    logger.error('Error processing session sync', error);
    res.status(500).json({ error: 'Internal server error during session sync' });
  }
}