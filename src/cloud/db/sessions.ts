import { PrismaClient } from '@prisma/client';

const prisma = new PrismaClient();

export interface SessionSyncPayload {
  session_id: string;
  user_id: string;
  start_time: string;
  end_time: string;
  event_count: number;
  duration_ms: number;
}

export interface UpsertResult {
  isNew: boolean;
  session_id: string;
}

/**
 * Upserts a session into the AiWorkSession table.
 * Implements a "grow-only" strategy:
 * - If the session exists, we only update if the new data indicates a longer duration 
 *   or higher event count (though deterministic ID usually implies a single logical session).
 * - Primarily, this ensures the row exists if it was missing due to the previous bug.
 */
export async function upsertSession(payload: SessionSyncPayload): Promise<UpsertResult> {
  const { session_id, user_id, start_time, end_time, event_count, duration_ms } = payload;

  // Parse times to ensure we are comparing numbers
  const newDuration = duration_ms;
  
  // Check if session exists
  const existingSession = await prisma.aiWorkSession.findUnique({
    where: { session_id },
  });

  if (existingSession) {
    // Grow-only logic: Update only if the new session is "larger" (more events or longer)
    // This handles cases where the collector might send incremental updates, 
    // though typically this is a final snapshot.
    const needsUpdate = 
      event_count > existingSession.event_count || 
      newDuration > existingSession.duration_ms;

    if (needsUpdate) {
      await prisma.aiWorkSession.update({
        where: { session_id },
        data: {
          event_count,
          duration_ms: newDuration,
          end_time, // Ensure end time is updated if the session was extended
        },
      });
      return { isNew: false, session_id };
    }
    
    return { isNew: false, session_id };
  }

  // If not exists, create it
  await prisma.aiWorkSession.create({
    data: {
      session_id,
      user_id,
      start_time,
      end_time,
      event_count,
      duration_ms,
    },
  });

  return { isNew: true, session_id };
}