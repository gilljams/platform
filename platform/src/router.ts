import { Kafka, Consumer, Producer, Partitioners } from "kafkajs";
import { getOrCreateCanonical, lookupCanonical, getMappings, getOrCreateDimensionMapping, applyDimRouting, addInboxItem, updateInboxItem, upsertDimensionCode, upsertCodeMapping, insertAuditEvent, isEventProcessed, markEventProcessed } from "./mapper";

const INGRESS = {
  ERP_PROJECTS: "erp.projects",
  ERP_ACCOUNTS: "erp.accounts",
  ERP_GENERAL_LEDGER: "erp.general-ledger",
  PRODUCT_A_EVENTS: "product-a.events",
  PRODUCT_A_TASKS: "product-a.tasks",
};

const EGRESS = {
  PROJECTS_OUT: "platform.projects.out",
  ACCOUNTS_OUT: "platform.accounts.out",
  BUDGET_OUT: "platform.budget.out",
  GL_OUT: "platform.gl.out",
  LINKS_OUT: "platform.links.out",
};

let producer: Producer;
let consumer: Consumer;

// In-memory event log (ring buffer)
const EVENT_LOG_MAX = 200;
const eventLog: Array<{ timestamp: string; direction: 'in' | 'out'; topic: string; event_type: string; canonical_id?: string; summary: string }> = [];

function logEvent(direction: 'in' | 'out', topic: string, event_type: string, canonical_id: string | undefined, summary: string, event_id?: string) {
  eventLog.unshift({ timestamp: new Date().toISOString(), direction, topic, event_type, canonical_id, summary });
  if (eventLog.length > EVENT_LOG_MAX) eventLog.length = EVENT_LOG_MAX;
  // Persist to audit_events table
  insertAuditEvent(direction, topic, event_type, event_id, canonical_id, summary);
}

export function getEventLog(limit = 100) {
  return eventLog.slice(0, limit);
}

async function publishEgress(topic: string, message: Record<string, unknown>) {
  const key = (message.canonical_id as string) || (message.event_id as string) || "unknown";
  await producer.send({
    topic,
    messages: [{ key, value: JSON.stringify(message) }],
  });
  logEvent('out', topic, (message.event_type as string) || (message.original as any)?.event_type || 'enriched', message.canonical_id as string | undefined, `→ ${topic} [${key}]`, (message.event_id as string) || (message.original as any)?.event_id);
  console.log(`[ROUTER] → ${topic}`);
}

async function handleMessage(topic: string, rawValue: string) {
  const event = JSON.parse(rawValue);

  // Idempotency: skip already-processed events
  const eventId = event.event_id;
  if (eventId && isEventProcessed(eventId)) {
    console.log(`[ROUTER] Skipping duplicate event: ${eventId}`);
    return;
  }

  logEvent('in', topic, event.event_type || 'unknown', undefined, `← ${topic}: ${event.event_type}`, eventId);
  console.log(`[ROUTER] ← ${topic}: ${event.event_type}`);

  switch (topic) {
    case INGRESS.ERP_ACCOUNTS: {
      // Referensdata — vidarebefordra utan berikning
      await publishEgress(EGRESS.ACCOUNTS_OUT, event);
      break;
    }

    case INGRESS.ERP_PROJECTS: {
      // Create canonical ID for ERP project
      const canonicalId = getOrCreateCanonical("erp", event.erp_id);
      // Register as dimension code + cross-reference
      upsertDimensionCode("project", canonicalId, event.name || event.erp_id);
      upsertCodeMapping("project", "erp", event.erp_id, canonicalId);
      await publishEgress(EGRESS.PROJECTS_OUT, {
        canonical_id: canonicalId,
        original: event,
      });
      break;
    }

    case INGRESS.ERP_GENERAL_LEDGER: {
      // Enrich with canonical_id based on erp_id + flex dimensions via dim_routing
      const canonicalId = lookupCanonical("erp", event.erp_id);
      if (canonicalId) {
        // Apply dim routing to each GL entry
        const entries = event.entries || [];
        const dimValuesPerEntry = entries.map((entry: Record<string, unknown>) =>
          applyDimRouting("erp", "prod_b", entry)
        );
        await publishEgress(EGRESS.GL_OUT, {
          canonical_id: canonicalId,
          dim_values_per_entry: dimValuesPerEntry,
          original: event,
        });
      } else {
        console.warn(`[ROUTER] No canonical ID for erp:${event.erp_id} — skipping GL`);
      }
      break;
    }

    case INGRESS.PRODUCT_A_EVENTS: {
      if (event.event_type === "BudgetProjectCreated") {
        const canonicalId = getOrCreateCanonical("prod_a", event.prod_a_id);
        // Register as dimension code + cross-reference
        upsertDimensionCode("project", canonicalId, event.name || event.prod_a_id);
        upsertCodeMapping("project", "prod_a", event.prod_a_id, canonicalId);
        await publishEgress(EGRESS.PROJECTS_OUT, {
          canonical_id: canonicalId,
          original: event,
        });
      } else if (event.event_type === "BudgetUpdated" || event.event_type === "BudgetSubmitted") {
        const canonicalId = lookupCanonical("prod_a", event.prod_a_id);
        if (canonicalId) {
          // Enrich with planning dimensions when submitting
          const enriched: Record<string, unknown> = {
            canonical_id: canonicalId,
            original: event,
          };
          if (event.event_type === "BudgetSubmitted" && event.version_name && event.year) {
            enriched.planning_dimensions = getOrCreateDimensionMapping(
              canonicalId, event.version_name, event.year, event.version_id
            );
          }
          // Apply dim routing to each budget line (same mechanism as GL)
          const lines = event.lines || [];
          const dimValuesPerLine = lines.map((line: Record<string, unknown>) =>
            applyDimRouting("prod_a", "prod_b", line)
          );
          if (dimValuesPerLine.some((dv: Record<string, string | null>) => dv.dim1 || dv.dim2 || dv.dim3 || dv.dim4 || dv.dim5)) {
            enriched.dim_values_per_line = dimValuesPerLine;
          }
          await publishEgress(EGRESS.BUDGET_OUT, enriched);
        } else {
          console.warn(`[ROUTER] No canonical ID for prod_a:${event.prod_a_id} — skipping budget`);
        }
      }
      break;
    }

    case INGRESS.PRODUCT_A_TASKS: {
      // Task lifecycle events from Product A
      if (event.event_type === "TaskAssigned") {
        addInboxItem({
          id: `${event.source}:${event.task_id}`,
          source: event.source,
          type: event.task_type || "action",
          category: event.category || "action",
          title: event.title,
          description: event.description,
          priority: event.priority || "normal",
          assigned_to: event.assigned_to,
          task_path: event.task_path,
          due_date: event.due_date,
        });
        logEvent('in', topic, 'TaskAssigned', undefined, `Task: "${event.title}" → ${event.assigned_to}`);
        console.log(`[ROUTER] Task assigned: "${event.title}" → ${event.assigned_to}`);
      } else if (event.event_type === "TaskCompleted") {
        updateInboxItem(`${event.source}:${event.task_id}`, { status: "done" });
        logEvent('in', topic, 'TaskCompleted', undefined, `Task completed: ${event.task_id}`);
        console.log(`[ROUTER] Task completed: ${event.task_id}`);
      }
      break;
    }
  }

  // Mark event as processed (idempotency)
  if (eventId) markEventProcessed(eventId);
}

export async function startRouter(kafka: Kafka) {
  producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
  consumer = kafka.consumer({ groupId: "platform-router" });

  await producer.connect();
  await consumer.connect();
  console.log("[ROUTER] Kafka connected");

  // Subscribe to all ingress topics
  for (const topic of Object.values(INGRESS)) {
    await consumer.subscribe({ topic, fromBeginning: true });
  }
  console.log("[ROUTER] Subscribed to:", Object.values(INGRESS).join(", "));

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (message.value) {
        try {
          await handleMessage(topic, message.value.toString());
        } catch (err) {
          console.error(`[ROUTER] Error processing ${topic}:`, err);
        }
      }
    },
  });
}

export async function publishLink(canonicalId: string, linked: Record<string, string>) {
  const event = {
    event_id: require("uuid").v4(),
    event_type: "ProjectLinked",
    timestamp: new Date().toISOString(),
    source_system: "platform",
    canonical_id: canonicalId,
    linked,
  };
  await publishEgress(EGRESS.LINKS_OUT, event);
  return event;
}
