// The end of a run: give its trigger's slot to the next waiting run, and fire the `event` triggers
// listening to this kind of ending. A workflow cannot start itself through events: a trigger whose
// workflow already appears in the chain of runs that led here is not fired.

import { type TriggerRow, fireTrigger, logTriggerEvent } from "./fire";
import { dispatchQueued, type Bindings, type Sql } from "./runs";

/** Longest chain of runs caused by one another before the next link is refused. */
export const MAX_EVENT_DEPTH = 5;

/** Workflow ids of the run and of every run that caused it, nearest first, with how deep the chain is. Pure part of the loop check. */
export function chainBlocks(chain: { workflow_id: string }[], targetWorkflowId: string): "loop_detected" | null {
  if (chain.length >= MAX_EVENT_DEPTH) return "loop_detected";
  return chain.some((link) => link.workflow_id === targetWorkflowId) ? "loop_detected" : null;
}

export async function handleRunFinished(sql: Sql, env: Bindings, runId: string): Promise<{ queuedStarted: number; fired: number; blocked: number }> {
  const [run] = await sql`
    select r.id, r.no, r.status, r.error, r.test, r.workspace_id, r.workflow_id, r.trigger_id, w.slug as workflow_slug, w.name as workflow_name
    from runs r join workflows w on w.id = r.workflow_id where r.id = ${runId}::uuid`;
  // A run started from the editor takes no slot and fires no event trigger.
  if (!run || run.test) return { queuedStarted: 0, fired: 0, blocked: 0 };

  const queuedStarted = run.trigger_id ? await dispatchQueued(sql, env, run.trigger_id) : 0;
  if (run.status !== "succeeded" && run.status !== "failed") return { queuedStarted, fired: 0, blocked: 0 };

  const triggers = (await sql`
    select t.id, t.type, t.enabled, t.workspace_id, t.workflow_id, t.auth_mode, t.auth_secret, t.filter, t.input_mapping,
      t.concurrency_policy, t.concurrency_max, t.rate_limit_per_minute, t.skip_next, w.status as workflow_status, w.current_version_id
    from triggers t join workflows w on w.id = t.workflow_id
    where t.workspace_id = ${run.workspace_id}::uuid and t.type = 'event' and t.enabled
      and t.event_type = ${`run.${run.status}`} and (t.event_workflow_id is null or t.event_workflow_id = ${run.workflow_id}::uuid)`) as unknown as TriggerRow[];
  if (triggers.length === 0) return { queuedStarted, fired: 0, blocked: 0 };

  const chain = (await sql`
    with recursive chain as (
      select id, workflow_id, cause_run_id, 1 as depth from runs where id = ${runId}::uuid
      union all
      select r.id, r.workflow_id, r.cause_run_id, c.depth + 1 from runs r join chain c on r.id = c.cause_run_id where c.depth < 10
    ) select workflow_id from chain`) as unknown as { workflow_id: string }[];

  const payload = {
    event: `run.${run.status}`,
    run: { id: run.id, no: Number(run.no), status: run.status, error: run.error ?? null, workflow: { id: run.workflow_id, slug: run.workflow_slug, name: run.workflow_name } },
  };
  let fired = 0;
  let blocked = 0;
  for (const trigger of triggers) {
    if (chainBlocks(chain, trigger.workflow_id)) {
      await logTriggerEvent(sql, trigger, "skipped", "loop_detected", null);
      blocked++;
      continue;
    }
    // The runner may announce the same ending twice (two steps failing together): one run per trigger is enough.
    const [already] = await sql`select 1 as done from runs where cause_run_id = ${runId}::uuid and trigger_id = ${trigger.id}::uuid limit 1`;
    if (already) continue;
    const outcome = await fireTrigger(sql, env, trigger, { body: payload, source: "event", causeRunId: runId });
    if (outcome.kind === "started") fired++;
  }
  return { queuedStarted, fired, blocked };
}
