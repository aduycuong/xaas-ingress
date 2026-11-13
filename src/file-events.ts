// A workspace file was added or given new content (by an upload or by a node): fire the `file.*` triggers whose path
// pattern matches. A file a run wrote carries that run as its cause, so a workflow cannot start itself again through the
// files it writes (the same chain check as run events).

import { chainBlocks } from "./events";
import { type TriggerRow, fireTrigger, logTriggerEvent } from "./fire";
import { matchesPathPattern } from "./path-pattern";
import type { Bindings, Sql } from "./runs";

export const fileEvents = ["file.created", "file.updated"] as const;
export type FileEvent = (typeof fileEvents)[number];

type FileRow = { id: string; workspace_id: string; folder: string; name: string; size: number; mime_type: string; origin: string; content_version: number; run_id: string | null; run_no: number | null };

async function loadFile(sql: Sql, fileId: string): Promise<FileRow | null> {
  const [row] = await sql`
    select f.id, f.workspace_id, f.folder, f.name, f.size::float8 as size, f.mime_type, f.origin, f.content_version, f.run_id, r.no as run_no
    from files f left join runs r on r.id = f.run_id where f.id = ${fileId}::uuid`;
  return (row as FileRow | undefined) ?? null;
}

const pathOf = (file: Pick<FileRow, "folder" | "name">) => (file.folder ? `${file.folder}/${file.name}` : file.name);

/** What a run started by a file receives as `{{ trigger.body… }}`. */
function payloadOf(event: FileEvent, file: FileRow) {
  return {
    event,
    file: {
      id: file.id,
      path: pathOf(file),
      name: file.name,
      folder: file.folder,
      size: Number(file.size),
      mime_type: file.mime_type,
      origin: file.origin,
      version: Number(file.content_version),
      run: file.run_id && file.run_no !== null ? { id: file.run_id, no: Number(file.run_no) } : null,
    },
  };
}

const TRIGGER_COLUMNS = `t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.auth_mode, t.auth_secret, t.filter, t.input_mapping,
  t.concurrency_policy, t.concurrency_max, t.rate_limit_per_minute, t.env, t.path_pattern, w.status as workflow_status, w.current_version_id`;

export async function handleFileEvent(sql: Sql, env: Bindings, message: { fileId: string; event: FileEvent; causeRunId: string | null }): Promise<{ fired: number; blocked: number; matched: number }> {
  const file = await loadFile(sql, message.fileId);
  // Deleted before the message arrived: nothing to start.
  if (!file) return { fired: 0, blocked: 0, matched: 0 };
  const path = pathOf(file);
  const triggers = ((await sql.query(
    `select ${TRIGGER_COLUMNS} from triggers t join workflows w on w.id = t.workflow_id
     where t.workspace_id = $1::uuid and t.type = 'event' and t.enabled and t.event_type = $2`,
    [file.workspace_id, message.event],
  )) as unknown as (TriggerRow & { path_pattern: string | null })[]).filter((trigger) => matchesPathPattern(trigger.path_pattern, path));
  if (!triggers.length) return { fired: 0, blocked: 0, matched: 0 };

  const chain = message.causeRunId
    ? ((await sql`
        with recursive chain as (
          select id, workflow_id, cause_run_id, 1 as depth from runs where id = ${message.causeRunId}::uuid
          union all
          select r.id, r.workflow_id, r.cause_run_id, c.depth + 1 from runs r join chain c on r.id = c.cause_run_id where c.depth < 10
        ) select workflow_id from chain`) as unknown as { workflow_id: string }[])
    : [];

  const payload = payloadOf(message.event, file);
  let fired = 0;
  let blocked = 0;
  for (const trigger of triggers) {
    if (chainBlocks(chain, trigger.workflow_id)) {
      await logTriggerEvent(sql, trigger, "skipped", "loop_detected", null);
      blocked++;
      continue;
    }
    // One run per trigger for one content of the file, whatever QStash delivers twice.
    const [already] = await sql`
      select 1 as done from runs where trigger_id = ${trigger.id}::uuid and cause_file_id = ${file.id}::uuid
        and (trigger_data #>> '{body,event}') = ${message.event} and (trigger_data #>> '{body,file,version}') = ${String(payload.file.version)} limit 1`;
    if (already) continue;
    const outcome = await fireTrigger(sql, env, trigger, { body: payload, source: "event", causeRunId: message.causeRunId, causeFileId: file.id });
    if (outcome.kind === "started") fired++;
  }
  return { fired, blocked, matched: triggers.length };
}

/** "Fire with this file" from the app: one trigger, one file of its workspace, as if the file had just been added. */
export async function fireWithFile(sql: Sql, env: Bindings, trigger: TriggerRow & { event_type?: string | null }, fileId: string, triggeredBy: string | null) {
  const file = await loadFile(sql, fileId);
  if (!file || file.workspace_id !== trigger.workspace_id) return null;
  const event = (fileEvents as readonly string[]).includes(trigger.event_type ?? "") ? (trigger.event_type as FileEvent) : "file.created";
  return fireTrigger(sql, env, trigger, { body: payloadOf(event, file), source: "api", triggeredBy, causeFileId: file.id });
}
