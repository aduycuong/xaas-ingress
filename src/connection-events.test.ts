import assert from "node:assert/strict";
import { test } from "node:test";
import { facebookEvents, facebookSignatureValid, sha256Hex, telegramChatOf, telegramEvent } from "./connection-events";
import { signBody } from "./crypto";

test("Telegram: a group message becomes telegram.message from a customer, keyed by bot and update", () => {
  const update = { update_id: 77, message: { message_id: 5, date: 1795512611, text: "hi", from: { id: 42, is_bot: false, first_name: "Lan", username: "lan" }, chat: { id: -100123, type: "supergroup", title: "Support" } } };
  const event = telegramEvent("999", update)!;
  assert.equal(event.type, "telegram.message");
  assert.equal(event.key, "999:77");
  assert.equal(event.role, "customer");
  assert.deepEqual(event.data.sender, { id: "42", name: "Lan", username: "lan", is_bot: false, role: "customer" });
  assert.deepEqual(event.data.chat, { id: "-100123", type: "supergroup", title: "Support" });
  assert.deepEqual(event.data.message, { id: "5", text: "hi", date: new Date(1795512611000).toISOString(), reply_to_message_id: null });
  assert.equal(event.raw, update);
});

test("Telegram: button presses and channel posts are events; edits and membership changes only teach chats", () => {
  const press = telegramEvent("9", { update_id: 1, callback_query: { id: "cb1", data: "ok", from: { id: 3, first_name: "A" }, message: { message_id: 8, chat: { id: 5, type: "private", first_name: "A" } } } })!;
  assert.equal(press.type, "telegram.callback_query");
  assert.deepEqual(press.data.callback, { id: "cb1", data: "ok" });
  assert.equal(telegramEvent("9", { update_id: 2, channel_post: { message_id: 1, date: 1, text: "news", chat: { id: -1009, type: "channel", title: "News" } } })!.type, "telegram.channel_post");
  const joined = { update_id: 3, my_chat_member: { chat: { id: -55, type: "group", title: "Ops" } } };
  assert.equal(telegramEvent("9", joined), null);
  assert.deepEqual(telegramChatOf(joined), { id: "-55", kind: "chat", label: "Ops", sub: "group" });
  assert.deepEqual(telegramChatOf({ update_id: 4, message: { chat: { id: 7, type: "private", first_name: "Bo" } } }), { id: "7", kind: "chat", label: "Bo", sub: "user" });
});

const PAGE = "1029";

test("Facebook: a comment by a customer, a reply, and the Page's own comment (self)", () => {
  const delivery = {
    object: "page",
    entry: [{
      id: PAGE,
      changes: [
        { field: "feed", value: { item: "comment", verb: "add", comment_id: "c1", post_id: "p1", parent_id: "p1", from: { id: "u1", name: "Minh" }, message: "giá?", created_time: 1795512610 } },
        { field: "feed", value: { item: "comment", verb: "add", comment_id: "c2", post_id: "p1", parent_id: "c1", from: { id: PAGE, name: "Shop" }, message: "inbox ạ" } },
        { field: "feed", value: { item: "comment", verb: "edited", comment_id: "c1", post_id: "p1" } },
        { field: "feed", value: { item: "reaction", verb: "add", post_id: "p1" } },
      ],
    }],
  };
  const [customer, self, ...rest] = facebookEvents(delivery, "app1");
  assert.equal(rest.length, 0, "edits and reactions are not events");
  assert.equal(customer.type, "facebook.comment");
  assert.equal(customer.key, `${PAGE}:comment:c1`);
  assert.equal(customer.role, "customer");
  assert.deepEqual(customer.data.comment, { id: "c1", parent_id: null, text: "giá?", created_at: new Date(1795512610000).toISOString() });
  assert.equal(self.role, "self");
  assert.equal((self.data.comment as { parent_id: string }).parent_id, "c1");
});

test("Facebook: Messenger messages, echoes (who sent them), postbacks and leads", () => {
  const delivery = {
    object: "page",
    entry: [{
      id: PAGE,
      messaging: [
        { sender: { id: "psid1" }, recipient: { id: PAGE }, timestamp: 1795512610000, message: { mid: "m1", text: "hello" } },
        { sender: { id: PAGE }, recipient: { id: "psid1" }, timestamp: 1795512611000, message: { mid: "m2", text: "hi!", is_echo: true, app_id: 111 } },
        { sender: { id: PAGE }, recipient: { id: "psid1" }, timestamp: 1795512612000, message: { mid: "m3", text: "typed by staff", is_echo: true } },
        { sender: { id: "psid1" }, recipient: { id: PAGE }, timestamp: 1795512613000, postback: { title: "Track", payload: "TRACK", mid: "m4" } },
        { sender: { id: "psid1" }, recipient: { id: PAGE }, delivery: { mids: ["m2"] } },
      ],
      changes: [{ field: "leadgen", value: { leadgen_id: "L1", form_id: "F1", ad_id: "A1", page_id: PAGE, created_time: 1795512614 } }],
    }],
  };
  const events = facebookEvents(delivery, "111");
  assert.deepEqual(events.map((event) => [event.type, event.role, event.key]), [
    ["facebook.lead", "customer", `${PAGE}:lead:L1`],
    ["facebook.message", "customer", `${PAGE}:message:m1`],
    ["facebook.message", "self", `${PAGE}:message:m2`],
    ["facebook.message", "self", `${PAGE}:message:m3`],
    ["facebook.postback", "customer", `${PAGE}:postback:m4`],
  ]);
  assert.equal(events[1].data.via, null);
  assert.equal(events[2].data.via, "xaas", "an echo of XAAS's own app");
  assert.equal(events[3].data.via, "inbox", "an echo without app: typed in the Page inbox");
  assert.deepEqual(events[4].data.postback, { title: "Track", payload: "TRACK", mid: "m4" });
  assert.equal(facebookEvents({ object: "instagram", entry: [] }, "111").length, 0);
});

test("Facebook signature and the hash of a Telegram secret", async () => {
  const raw = '{"object":"page","entry":[]}';
  const signature = await signBody("appsecret", raw);
  assert.equal(await facebookSignatureValid(raw, signature, "appsecret"), true);
  assert.equal(await facebookSignatureValid(raw + " ", signature, "appsecret"), false);
  assert.equal(await facebookSignatureValid(raw, undefined, "appsecret"), false);
  assert.equal(await sha256Hex("abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
});
