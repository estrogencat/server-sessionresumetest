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

import { OPCODES, Payload, Send, tryResume, WebSocket } from "@spacebar/gateway";
import { checkToken, EVENTEnum, Intents } from "@spacebar/util";
import { setupListener } from "../listener/listener";
import { Capabilities } from "../util/Capabilities";

interface ResumeSchema {
    token: string;
    session_id: string;
    seq: number;
}

// same fallback Identify.ts uses when a client doesn't send explicit intents.
// resume payloads don't carry intents at all, so this is the best we can do
// without keeping the original Identify data around indefinitely.
const DEFAULT_INTENTS = 0b11011111111111111111111111111111111n;

async function invalidSession(socket: WebSocket) {
    await Send(socket, {
        op: OPCODES.Invalid_Session,
        d: false,
    });
}

export async function onResume(this: WebSocket, data: Payload) {
    const resume = data.d as Partial<ResumeSchema> | undefined;

    if (!resume?.token || !resume?.session_id || typeof resume.seq !== "number") {
        return invalidSession(this);
    }

    let tokenData;
    try {
        tokenData = await checkToken(resume.token, { ipAddress: this.ipAddress, fingerprint: this.fingerprint });
    } catch {
        return invalidSession(this);
    }

    const user = tokenData.user;
    if (!user) return invalidSession(this);

    // the session_id the client is trying to resume has to be the one tied
    // to this token (i.e. the same "device") - anything else is either a
    // stale/forged session_id or a session belonging to another login
    if (!tokenData.session || tokenData.session.session_id !== resume.session_id) {
        return invalidSession(this);
    }

    const replay = tryResume(resume.session_id, user.id, resume.seq);
    if (replay === null) {
        // unknown session, expired grace period, or already resumed/attached
        // elsewhere - client needs to do a full IDENTIFY instead
        return invalidSession(this);
    }

    clearTimeout(this.readyTimeout);

    this.accessToken = resume.token;
    this.user_id = user.id;
    this.session_id = tokenData.session.session_id;
    this.session = tokenData.session;
    this.intents ??= new Intents(DEFAULT_INTENTS);
    this.capabilities ??= new Capabilities(0);
    this.large_threshold ||= 250;
    this.permissions ??= {};
    this.events ??= {};
    this.member_events ??= {};
    this.recentTransactions ??= [];

    let lastSeq = resume.seq;
    for (const payload of replay) {
        await Send(this, payload);
        if (typeof payload.s === "number") lastSeq = payload.s;
    }
    this.sequence = lastSeq + 1;

    // re-establish live subscriptions on the new socket - this is the same
    // subscription setup a fresh IDENTIFY does, just without re-sending READY
    await setupListener.call(this);

    await Send(this, {
        op: OPCODES.Dispatch,
        t: EVENTEnum.Resumed,
        s: this.sequence++,
        d: {},
    });

    console.log(`[Gateway/${this.user_id}] Resumed session ${this.session_id}, replayed ${replay.length} missed event(s)`);
}
