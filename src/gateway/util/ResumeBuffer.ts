/*
	Spacebar: A FOSS re-implementation and extension of the Discord.com backend.
	Copyright (C) 2023 Spacebar and Spacebar Contributors

	This program is free software: you can redistribute it and/or modify
	it under the terms of the GNU Affero General Public License as published
	by the Free Software Foundation, either version 3 of the License, or
	(at your option) any later version.

	This program is distributed in the hope that it will be useful,
	but WITHOUT ANY WARRANTY; without even the implied warranty of
	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
	GNU Affero General Public License for more details.

	You should have received a copy of the GNU Affero General Public License
	along with this program.  If not, see <https://www.gnu.org/licenses/>.
*/

import { Payload, WebSocket } from "@spacebar/gateway";

// How long a disconnected session stays resumable. Real Discord's window is a
// few minutes; we keep this conservative since events are held in memory.
const RESUME_GRACE_MS = 90 * 1000;

// How many dispatches we keep per session. If a client is offline long enough
// to miss more than this, they'll fall back to a full re-identify - same as
// what happens today, just for a much narrower window.
const MAX_BUFFERED_DISPATCHES = 250;

interface SessionEntry {
    user_id: string;
    buffer: Payload[];
    // set when the socket disconnects; cleared (entry deleted) once either
    // resumed or the grace period elapses
    disconnectedAt: number | null;
    cleanupTimer: ReturnType<typeof setTimeout> | null;
}

const sessions = new Map<string, SessionEntry>();

function getOrCreateEntry(session_id: string, user_id: string): SessionEntry {
    let entry = sessions.get(session_id);
    if (!entry) {
        entry = { user_id, buffer: [], disconnectedAt: null, cleanupTimer: null };
        sessions.set(session_id, entry);
    }
    return entry;
}

// call this whenever a Dispatch payload (one with a sequence number) is sent
// to a socket, so it's available for replay if the connection later drops
export function recordDispatch(socket: WebSocket, payload: Payload) {
    if (!socket.session_id || !socket.user_id || typeof payload.s !== "number") return;

    const entry = getOrCreateEntry(socket.session_id, socket.user_id);
    entry.buffer.push(payload);
    if (entry.buffer.length > MAX_BUFFERED_DISPATCHES) entry.buffer.shift();
}

// call this when a socket closes - starts the grace-period clock. If nobody
// resumes this session before it elapses, the buffer is dropped for good.
export function markDisconnected(session_id?: string) {
    if (!session_id) return;
    const entry = sessions.get(session_id);
    if (!entry) return;

    entry.disconnectedAt = Date.now();
    if (entry.cleanupTimer) clearTimeout(entry.cleanupTimer);
    entry.cleanupTimer = setTimeout(() => {
        const current = sessions.get(session_id);
        if (current === entry) sessions.delete(session_id);
    }, RESUME_GRACE_MS);
    entry.cleanupTimer.unref?.();
}

// attempts to resume: returns the missed dispatches (in order, seq > clientSeq)
// on success, or null if this session can't be resumed (unknown, expired, or
// belongs to a different user than the token resolved to).
export function tryResume(session_id: string, user_id: string, clientSeq: number): Payload[] | null {
    const entry = sessions.get(session_id);
    if (!entry) return null;
    if (entry.user_id !== user_id) return null;

    // session is still attached to a live socket (shouldn't normally happen -
    // a second connection resuming a still-open session is not valid)
    if (entry.disconnectedAt === null) return null;

    if (entry.cleanupTimer) clearTimeout(entry.cleanupTimer);
    sessions.delete(session_id);

    return entry.buffer.filter((p) => (p.s ?? 0) > clientSeq);
}
