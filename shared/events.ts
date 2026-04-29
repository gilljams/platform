// ── Kafka Topics ──

// Ingress (sources → Platform)
export const TOPICS_INGRESS = {
  ERP_PROJECTS: "erp.projects",
  ERP_ACCOUNTS: "erp.accounts",
  ERP_GENERAL_LEDGER: "erp.general-ledger",
  PRODUCT_A_EVENTS: "product-a.events",
} as const;

// Egress (Platform → products)
export const TOPICS_EGRESS = {
  PROJECTS_OUT: "platform.projects.out",
  ACCOUNTS_OUT: "platform.accounts.out",
  BUDGET_OUT: "platform.budget.out",
  GL_OUT: "platform.gl.out",
  ENTITY_LINKED_OUT: "platform.entity-linked.out",
} as const;

export const ALL_TOPICS = [
  ...Object.values(TOPICS_INGRESS),
  ...Object.values(TOPICS_EGRESS),
];

// ── ID Prefixes ──
export const ID_PREFIX = {
  PLATFORM: "platform-",
  ERP: "erp-",
  PROD_A: "prod_a-",
  PROD_B: "prod_b-",
} as const;

// ── Base Event ──
export interface BaseEvent {
  event_id: string;
  event_type: string;
  timestamp: string;
  source_system: "erp" | "prod_a" | "prod_b" | "platform";
}

// ── ERP Events ──

export interface Account {
  code: string;
  name: string;
}

export interface OrgUnit {
  code: string;
  name: string;
}

export interface AccountsPublished extends BaseEvent {
  event_type: "AccountsPublished";
  source_system: "erp";
  accounts: Account[];
  org_units: OrgUnit[];
}

export interface ProjectCreated extends BaseEvent {
  event_type: "ProjectCreated";
  source_system: "erp";
  erp_id: string;
  name: string;
}

export interface GeneralLedgerEntry {
  account: string;
  org_unit: string;
  amount: number;
  currency: string;
  period: string;
  transaction_date?: string;
  activity?: string;
  cost_bearer?: string;
  counterpart?: string;
}

export interface GeneralLedgerPublished extends BaseEvent {
  event_type: "GeneralLedgerPublished";
  source_system: "erp";
  erp_id: string;
  entries: GeneralLedgerEntry[];
}

// ── Product A Events ──

export interface BudgetProjectCreated extends BaseEvent {
  event_type: "BudgetProjectCreated";
  source_system: "prod_a";
  prod_a_id: string;
  name: string;
}

export interface BudgetLine {
  account: string;
  org_unit: string;
  amount: number;
  currency: string;
  period: string;
  activity?: string;
  cost_bearer?: string;
  counterpart?: string;
}

export interface BudgetUpdated extends BaseEvent {
  event_type: "BudgetUpdated";
  source_system: "prod_a";
  prod_a_id: string;
  lines: BudgetLine[];
}

export interface BudgetSubmitted extends BaseEvent {
  event_type: "BudgetSubmitted";
  source_system: "prod_a";
  prod_a_id: string;
  version_id: string;
  version_name: string;
  year: string;
  lines: BudgetLine[];
}

// ── Platform Events ──

export interface EntityLinked extends BaseEvent {
  event_type: "EntityLinked";
  source_system: "platform";
  dimension: string;
  entities: Array<{
    source_system: string;
    source_key: string;
    name: string;
  }>;
}

// ── Enriched wrapper (Platform adds source identity) ──
export interface EnrichedEvent<T extends BaseEvent> {
  source_system: string;
  source_key: string;
  original: T;
}

// ── Union types ──
export type ErpEvent = AccountsPublished | ProjectCreated | GeneralLedgerPublished;
export type ProductAEvent = BudgetProjectCreated | BudgetUpdated | BudgetSubmitted;
export type PlatformEvent = EntityLinked;
export type AnyEvent = ErpEvent | ProductAEvent | PlatformEvent;
