// Events of connections: what a Telegram bot or a Facebook Page receives, sent here by the service (`/hooks/telegram/:botId`,
// `/hooks/facebook`). The app subscribes each account once (`connection_subscriptions`), whatever number of workspaces
// connected it; here each delivery is checked, turned into one event per thing that happened, remembered so a delivery
// sent again starts nothing twice (`inbound_event_receipts`), and handed to the `event` triggers of every connection of
// the account that emits that event type (`connections.events`). The shape of an event is the contract of
// xaas-workflows/lib/connections/event-samples.ts: change both together.

import { sameSecret, signBody } from "./crypto";
import { type TriggerRow, fireTrigger } from "./fire";
import type { Bindings, Sql } from "./runs";

export type ConnectionEventType =
  | "telegram.message"
  | "telegram.channel_post"
  | "telegram.callback_query"
  | "facebook.comment"
  | "facebook.message"
  | "facebook.postback"
  | "facebook.lead";

/** Who sent it: `customer` is anyone else, `self` the account itself (only passed on to connections that ask for it). */
export type SenderRole = "customer" | "self";

/** One thing that happened on an account, before it is given to the connections that emit it. */
export type InboundEvent = {
  provider: "telegram" | "facebook-page";
  /** The account on the service: the bot id, the Page id. */
  account: string;
  type: ConnectionEventType;
  /** Unique per provider: `<account>:<id of the event on the service>`. */
  key: string;
  role: SenderRole;
  /** `sender`, `chat`, `message`… : what the event says, in the order a workflow reads it. */
  data: Record<string, unknown>;
  /** What the service sent for it, untouched. */
  raw: unknown;
};

/** A chat a bot is in, learned from an update: added to the resources of the bot's connections (getUpdates cannot run while a webhook is set). */
export type LearnedChat = { id: string; kind: "chat"; label: string; sub: string };

type Obj = Record<string, any>;
const isObj = (value: unknown): value is Obj => value !== null && typeof value === "object" && !Array.isArray(value);
const str = (value: unknown) => (value === undefined || value === null ? null : String(value));
const isoFromSeconds = (seconds: unknown) => (typeof seconds === "number" ? new Date(seconds * 1000).toISOString() : null);
const isoFromMillis = (millis: unknown) => (typeof millis === "number" ? new Date(millis).toISOString() : null);

// ---------- Telegram ----------

function tgName(user: Obj): string {
  return [user.first_name, user.last_name].filter(Boolean).join(" ") || user.title || (user.username ? `@${user.username}` : String(user.id));
}
const tgChat = (chat: Obj) => ({ id: String(chat.id), type: chat.type ?? null, title: chat.title ?? (tgName(chat) || null) });
const tgUser = (user: Obj) => ({ id: String(user.id), name: tgName(user), username: user.username ?? null, is_bot: Boolean(user.is_bot) });
function tgMessage(message: Obj) {
  return { id: String(message.message_id), text: message.text ?? message.caption ?? null, date: isoFromSeconds(message.date), reply_to_message_id: str(message.reply_to_message?.message_id) };
}

/** The chat of an update, as a resource of the bot's connections. */
export function telegramChatOf(update: Obj): LearnedChat | null {
  const chat = update.message?.chat ?? update.edited_message?.chat ?? update.channel_post?.chat ?? update.my_chat_member?.chat ?? update.callback_query?.message?.chat;
  if (!isObj(chat) || chat.id === undefined) return null;
  return { id: String(chat.id), kind: "chat", label: tgChat(chat).title ?? String(chat.id), sub: chat.type === "private" ? "user" : String(chat.type ?? "chat") };
}

/** The event an update of a bot carries, or null (edits, membership changes: only used to learn chats). Pure. */
export function telegramEvent(botId: string, update: Obj): InboundEvent | null {
  const key = `${botId}:${update.update_id}`;
  const base = { provider: "telegram" as const, account: botId, key, role: "customer" as const, raw: update };
  if (isObj(update.message)) {
    const message = update.message;
    const sender = isObj(message.from) ? tgUser(message.from) : isObj(message.sender_chat) ? { id: String(message.sender_chat.id), name: tgName(message.sender_chat), username: message.sender_chat.username ?? null, is_bot: false } : null;
    return { ...base, type: "telegram.message", data: { sender: sender && { ...sender, role: "customer" }, chat: tgChat(message.chat), message: tgMessage(message) } };
  }
  if (isObj(update.channel_post)) {
    const post = update.channel_post;
    const chat = tgChat(post.chat);
    return { ...base, type: "telegram.channel_post", data: { sender: { id: chat.id, name: chat.title, username: post.chat.username ?? null, is_bot: false, role: "customer" }, chat, message: tgMessage(post) } };
  }
  if (isObj(update.callback_query)) {
    const query = update.callback_query;
    return {
      ...base,
      type: "telegram.callback_query",
      data: {
        sender: { ...tgUser(query.from ?? {}), role: "customer" },
        chat: isObj(query.message?.chat) ? tgChat(query.message.chat) : null,
        message: isObj(query.message) ? { id: String(query.message.message_id) } : null,
        callback: { id: String(query.id), data: query.data ?? null },
      },
    };
  }
  return null;
}

// ---------- Facebook ----------

/** The events of one webhook delivery of the Page object (several entries, several changes or messages each). Pure. */
export function facebookEvents(body: unknown, appId: string | undefined): InboundEvent[] {
  if (!isObj(body) || body.object !== "page" || !Array.isArray(body.entry)) return [];
  const events: InboundEvent[] = [];
  for (const entry of body.entry) {
    if (!isObj(entry)) continue;
    const page = String(entry.id);
    const base = { provider: "facebook-page" as const, account: page };
    for (const change of Array.isArray(entry.changes) ? entry.changes : []) {
      if (!isObj(change) || !isObj(change.value)) continue;
      const value = change.value;
      if (change.field === "feed" && value.item === "comment" && value.verb === "add" && value.comment_id) {
        const from = isObj(value.from) ? value.from : {};
        const role: SenderRole = String(from.id) === page ? "self" : "customer";
        // A reply to a comment has that comment as parent; a top-level comment has the post.
        const parent = value.parent_id && value.parent_id !== value.post_id ? String(value.parent_id) : null;
        events.push({
          ...base,
          type: "facebook.comment",
          key: `${page}:comment:${value.comment_id}`,
          role,
          data: {
            sender: { id: str(from.id), name: from.name ?? null, role },
            post: { id: str(value.post_id) },
            comment: { id: String(value.comment_id), parent_id: parent, text: value.message ?? null, created_at: isoFromSeconds(value.created_time) },
          },
          raw: change,
        });
      } else if (change.field === "leadgen" && value.leadgen_id) {
        events.push({
          ...base,
          type: "facebook.lead",
          key: `${page}:lead:${value.leadgen_id}`,
          role: "customer",
          data: { sender: null, lead: { id: String(value.leadgen_id), form_id: str(value.form_id), ad_id: str(value.ad_id), adgroup_id: str(value.adgroup_id), page_id: str(value.page_id ?? page), created_at: isoFromSeconds(value.created_time) } },
          raw: change,
        });
      }
    }
    for (const item of Array.isArray(entry.messaging) ? entry.messaging : []) {
      if (!isObj(item)) continue;
      if (isObj(item.message) && item.message.mid) {
        const message = item.message;
        const echo = Boolean(message.is_echo);
        // An echo is what the Page sent: the sender is the Page, the customer is the recipient.
        const role: SenderRole = echo ? "self" : "customer";
        const via = !echo ? null : message.app_id === undefined || message.app_id === null ? "inbox" : appId && String(message.app_id) === appId ? "xaas" : "app";
        const attachments = Array.isArray(message.attachments) ? message.attachments.map((file: Obj) => ({ type: file?.type ?? null, url: file?.payload?.url ?? null })) : [];
        events.push({
          ...base,
          type: "facebook.message",
          key: `${page}:message:${message.mid}`,
          role,
          data: {
            sender: { id: str(item.sender?.id), name: null, role },
            recipient: { id: str(item.recipient?.id) },
            message: { id: String(message.mid), text: message.text ?? null, attachments, quick_reply_payload: message.quick_reply?.payload ?? null, reply_to_id: message.reply_to?.mid ?? null, sent_at: isoFromMillis(item.timestamp) },
            via,
          },
          raw: item,
        });
      } else if (isObj(item.postback)) {
        const postback = item.postback;
        events.push({
          ...base,
          type: "facebook.postback",
          key: `${page}:postback:${postback.mid ?? `${item.sender?.id}:${item.timestamp}`}`,
          role: "customer",
          data: { sender: { id: str(item.sender?.id), name: null, role: "customer" }, recipient: { id: str(item.recipient?.id) }, postback: { title: postback.title ?? null, payload: postback.payload ?? null, mid: postback.mid ?? null } },
          raw: item,
        });
      }
    }
  }
  return events;
}

/** Whether `X-Hub-Signature-256` is the HMAC-SHA256 of the raw body under the app secret. */
export async function facebookSignatureValid(rawBody: string, header: string | undefined, appSecret: string): Promise<boolean> {
  if (!header) return false;
  return sameSecret(header.trim().toLowerCase(), await signBody(appSecret, rawBody));
}

/** SHA-256 (hex) of a text: what the app keeps of a Telegram `secret_token`. */
export async function sha256Hex(text: string): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(text)));
  return Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

// ---------- Handing events to triggers ----------

const TRIGGER_COLUMNS = `t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.auth_mode, t.auth_secret, t.filter, t.input_mapping,
  t.concurrency_policy, t.concurrency_max, t.rate_limit_per_minute, t.env, t.event_type, w.status as workflow_status, w.current_version_id`;

export type EmitResult = { duplicate: boolean; connections: number; fired: number };

/**
 * Gives one event to the connections of its account that emit its type (a `self` event only to those that ask for
 * them), and fires their enabled `event` triggers. A delivery seen before starts nothing. If firing fails the delivery
 * is forgotten again, so the service's retry gets another chance.
 */
export async function emitConnectionEvent(sql: Sql, env: Bindings, event: InboundEvent, receivedAt = new Date()): Promise<EmitResult> {
  const [fresh] = await sql`insert into inbound_event_receipts (provider, event_key) values (${event.provider}, ${event.key}) on conflict do nothing returning 1 as ok`;
  if (!fresh) return { duplicate: true, connections: 0, fired: 0 };
  try {
    const listeners = (await sql`
      select c.id, c.name, c.account_label
      from connections c
      where c.provider = ${event.provider} and c.external_id = ${event.account} and c.status <> 'error'
        and (c.events -> 'types') ? ${event.type}
        and (${event.role}::text = 'customer' or coalesce((c.events ->> 'fromSelf')::boolean, false))`) as { id: string; name: string; account_label: string }[];
    let fired = 0;
    for (const connection of listeners) {
      const triggers = (await sql.query(
        `select ${TRIGGER_COLUMNS} from triggers t join workflows w on w.id = t.workflow_id
         where t.type = 'event' and t.enabled and t.event_type = $1 and t.event_connection_id = $2::uuid`,
        [event.type, connection.id],
      )) as unknown as TriggerRow[];
      if (!triggers.length) continue;
      const body = {
        event: event.type,
        id: event.key,
        received_at: receivedAt.toISOString(),
        connection: { id: connection.id, name: connection.name, provider: event.provider, account: connection.account_label },
        ...event.data,
        raw: event.raw,
      };
      for (const trigger of triggers) {
        const outcome = await fireTrigger(sql, env, trigger, { body, source: "event" });
        if (outcome.kind === "started") fired++;
      }
    }
    return { duplicate: false, connections: listeners.length, fired };
  } catch (error) {
    await sql`delete from inbound_event_receipts where provider = ${event.provider} and event_key = ${event.key}`.then(undefined, () => undefined);
    throw error;
  }
}

/** Records that the account's subscription delivered something (the drawer shows "Last event"). */
export async function touchSubscription(sql: Sql, provider: string, account: string) {
  await sql`update connection_subscriptions set last_event_at = now() where provider = ${provider} and external_id = ${account}`;
}

/** Adds a chat the bot is in to the resources of each connection of the bot that does not list it yet. */
export async function learnTelegramChat(sql: Sql, botId: string, chat: LearnedChat) {
  await sql`
    update connections set resources = resources || ${JSON.stringify([chat])}::jsonb, updated_at = now()
    where provider = 'telegram' and external_id = ${botId} and not (resources @> ${JSON.stringify([{ id: chat.id }])}::jsonb)`;
}
