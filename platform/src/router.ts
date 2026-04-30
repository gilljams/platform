import { Kafka, Consumer, Producer, Partitioners } from "kafkajs";
import { getOrCreateDimensionMapping, applyDimRouting, addInboxItem, updateInboxItem, upsertEconEntity, upsertEconRelation, insertAuditEvent, isEventProcessed, markEventProcessed } from "./mapper";

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
  ENTITY_LINKED_OUT: "platform.entity-linked.out",
};

let producer: Producer;
let consumer: Consumer;

// In-memory event log (ring buffer)
const EVENT_LOG_MAX = 200;
const eventLog: Array<{ timestamp: string; direction: 'in' | 'out'; topic: string; event_type: string; source_key?: string; summary: string }> = [];

function logEvent(direction: 'in' | 'out', topic: string, event_type: string, source_key: string | undefined, summary: string, event_id?: string) {
  eventLog.unshift({ timestamp: new Date().toISOString(), direction, topic, event_type, source_key, summary });
  if (eventLog.length > EVENT_LOG_MAX) eventLog.length = EVENT_LOG_MAX;
  // Persist to audit_events table
  insertAuditEvent(direction, topic, event_type, event_id, source_key, summary);
}

export function getEventLog(limit = 100) {
  return eventLog.slice(0, limit);
}

async function publishEgress(topic: string, message: Record<string, unknown>) {
  // Stamp each egress message with a unique event_id to avoid idempotency collisions
  // when the same original event is published to multiple egress topics
  if (!message.event_id) {
    message.event_id = require("uuid").v4();
  }
  const key = (message.source_key as string) || (message.event_id as string) || "unknown";
  await producer.send({
    topic,
    messages: [{ key, value: JSON.stringify(message) }],
  });
  logEvent('out', topic, (message.event_type as string) || (message.original as any)?.event_type || 'enriched', message.source_key as string | undefined, `→ ${topic} [${key}]`, (message.event_id as string) || (message.original as any)?.event_id);
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
      console.log(`[ROUTER] Forwarded accounts: ${(event.accounts || []).length} accounts + ${(event.org_units || []).length} org_units`);
      break;
    }

    case INGRESS.ERP_PROJECTS: {
      // Route with source_system + source_key (no canonical ID creation)
      await publishEgress(EGRESS.PROJECTS_OUT, {
        source_system: "erp",
        source_key: event.erp_id,
        name: event.name || event.erp_id,
        original: event,
      });
      break;
    }

    case INGRESS.ERP_GENERAL_LEDGER: {
      // Enrich with flex dimensions via dim_routing, route with source identity
      const entries = event.entries || [];
      const dimValuesPerEntry = entries.map((entry: Record<string, unknown>) =>
        applyDimRouting("erp", "prod_b", entry)
      );
      await publishEgress(EGRESS.GL_OUT, {
        source_system: "erp",
        source_key: event.erp_id,
        dim_values_per_entry: dimValuesPerEntry,
        original: event,
      });
      break;
    }

    case INGRESS.PRODUCT_A_EVENTS: {
      if (event.event_type === "BudgetProjectCreated") {
        await publishEgress(EGRESS.PROJECTS_OUT, {
          source_system: "prod_a",
          source_key: event.prod_a_id,
          name: event.name || event.prod_a_id,
          original: event,
        });
      } else if (event.event_type === "BudgetUpdated" || event.event_type === "BudgetSubmitted") {
        const sourceKey = event.prod_a_id;
        const enriched: Record<string, unknown> = {
          source_system: "prod_a",
          source_key: sourceKey,
          original: event,
        };
        if (event.event_type === "BudgetSubmitted" && event.version_name && event.year) {
          enriched.planning_dimensions = getOrCreateDimensionMapping(
            "prod_a", sourceKey, event.version_name, event.year, event.version_id
          );
          // Ensure a project exists in downstream consumers for this source_key
          await publishEgress(EGRESS.PROJECTS_OUT, {
            source_system: "prod_a",
            source_key: sourceKey,
            name: event.version_name || sourceKey,
            original: event,
          });
        }
        // Apply dim routing to each budget line
        const lines = event.lines || [];
        const dimValuesPerLine = lines.map((line: Record<string, unknown>) =>
          applyDimRouting("prod_a", "prod_b", line)
        );
        if (dimValuesPerLine.some((dv: Record<string, string | null>) => dv.dim1 || dv.dim2 || dv.dim3 || dv.dim4 || dv.dim5)) {
          enriched.dim_values_per_line = dimValuesPerLine;
        }
        await publishEgress(EGRESS.BUDGET_OUT, enriched);
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

export async function publishEntityLinked(dimension: string, entities: Array<{source_system: string; source_key: string; name: string}>) {
  const event = {
    event_id: require("uuid").v4(),
    event_type: "EntityLinked",
    timestamp: new Date().toISOString(),
    source_system: "economy_domain",
    dimension,
    entities,
  };
  await publishEgress(EGRESS.ENTITY_LINKED_OUT, event);
  return event;
}
