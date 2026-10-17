// The ingestion loop, extracted so it can be driven by a misbehaving ledger in
// tests. `run()` in index.ts owns the process, the HTTP surface and the timer;
// this module owns one poll: fetch pages until the cursor stops advancing,
// de-duplicating events and refusing to let a confused RPC node move the
// indexer backwards.
//
// Why the guards exist (see the chaos suite in `chaos.test.ts`): Soroban RPC
// presents non-final states, cursor regressions and stale `latestLedger` values.
// None of them may rewind stored state or spin the loop forever.

import type { DecodedEvent, RpcEvent } from "./types.js";

export interface EventsPage {
  events: RpcEvent[];
  cursor: string;
  latestLedger: number;
}

/** The RPC surface this loop needs — the real client satisfies it structurally. */
export interface LedgerReader {
  getEvents(params: Record<string, unknown>): Promise<EventsPage>;
  getLatestLedger(): Promise<{ sequence: number }>;
}

/** The storage surface: the real `StreamStore` satisfies it structurally. */
export interface PageSink {
  getState(key: string): string | undefined;
  ingestPage(events: DecodedEvent[], cursor: string | undefined, latestLedger: number): void;
}

export type PollAlertKind =
  | "cursor_regression"
  | "duplicate_page"
  | "ledger_gap"
  | "rpc_divergence";

export interface PollAlert {
  kind: PollAlertKind;
  detail: string;
}

export interface PollOptions {
  /** Used only on the very first poll, when no cursor is stored yet. */
  startLedger?: number;
  filters: unknown[];
  pageLimit?: number;
  /** Stops a pathological node from paging forever in a single tick. */
  maxPages?: number;
  decode: (event: RpcEvent) => DecodedEvent;
  onAlert?: (alert: PollAlert) => void;
}

export interface PollResult {
  ingested: number;
  duplicates: number;
  cursor: string | null;
  pages: number;
}

const DEFAULT_PAGE_LIMIT = 1000;
const DEFAULT_MAX_PAGES = 50;

/**
 * One poll. Returns what it ingested so a caller can log/measure it; never
 * throws for an RPC anomaly (those become alerts), only for real transport or
 * decode failures, which the caller retries on the next tick.
 */
export async function pollOnce(
  reader: LedgerReader,
  sink: PageSink,
  options: PollOptions,
): Promise<PollResult> {
  const pageLimit = options.pageLimit ?? DEFAULT_PAGE_LIMIT;
  const maxPages = options.maxPages ?? DEFAULT_MAX_PAGES;
  const alert = (kind: PollAlertKind, detail: string): void => options.onAlert?.({ kind, detail });

  let cursor = sink.getState("cursor") ?? null;
  let startLedger: number | undefined;
  if (!cursor) {
    startLedger = options.startLedger ?? (await reader.getLatestLedger()).sequence;
  }

  const seenEventIds = new Set<string>();
  const usedCursors = new Set<string>();
  let lastEventLedger: number | null = null;
  let lastLatestLedger: number | null = null;
  let ingested = 0;
  let duplicates = 0;
  let pages = 0;

  for (; pages < maxPages; ) {
    const params: Record<string, unknown> = {
      ...(cursor ? { cursor } : { startLedger }),
      filters: options.filters,
      pagination: { limit: pageLimit },
      xdrFormat: "base64",
    };
    const page = await reader.getEvents(params);
    pages += 1;

    // A node that answers with the cursor we just asked with, and nothing new,
    // is stuck: paginating again would spin until maxPages for no progress.
    const nextCursor = page.cursor || cursor || null;
    if (nextCursor && usedCursors.has(nextCursor)) {
      alert("duplicate_page", `cursor ${nextCursor} returned twice; stopping this poll`);
      break;
    }
    if (nextCursor) usedCursors.add(nextCursor);

    // `latestLedger` must never move backwards: a node behind what we have
    // already ingested is a divergent replica, not progress.
    if (lastLatestLedger !== null && page.latestLedger < lastLatestLedger) {
      alert(
        "rpc_divergence",
        `latestLedger went ${lastLatestLedger} -> ${page.latestLedger}; keeping local state`,
      );
    }
    const effectiveLatest = Math.max(page.latestLedger, lastLatestLedger ?? page.latestLedger);
    lastLatestLedger = effectiveLatest;

    const fresh: DecodedEvent[] = [];
    for (const raw of page.events) {
      if (seenEventIds.has(raw.id)) {
        duplicates += 1;
        continue;
      }
      seenEventIds.add(raw.id);

      const decoded = options.decode(raw);
      // Events older than one we already accepted mean the page went backwards.
      if (lastEventLedger !== null && decoded.ledger < lastEventLedger) {
        alert(
          "ledger_gap",
          `event ${decoded.id} at ledger ${decoded.ledger} after ${lastEventLedger}; skipped`,
        );
        continue;
      }
      lastEventLedger = decoded.ledger;
      fresh.push(decoded);
    }

    if (fresh.length > 0 || nextCursor !== cursor) {
      sink.ingestPage(fresh, nextCursor ?? undefined, effectiveLatest);
      ingested += fresh.length;
    }

    cursor = nextCursor;
    if (page.events.length < pageLimit || !cursor) break;
  }

  return { ingested, duplicates, cursor, pages };
}
