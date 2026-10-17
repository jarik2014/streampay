// Chaos suite for the ingestion loop (#223). The ledger is scripted, not mocked
// with a library: each test hands `pollOnce` a `LedgerReader` that returns the
// pages a misbehaving node would return, and asserts what the loop did with
// them. Run with the package's own runner (`npm test`), which builds first.
//
// The four failures a Soroban RPC endpoint can present:
//   * cursor rewind      — the same page comes back forever
//   * late/dropped tx     — an event already ingested reappears, or the page
//                          repeats ids
//   * ledger-gap skip     — an event carrying an older ledger than one already
//                          accepted
//   * replica divergence  — `latestLedger` behind what this indexer already saw

import { describe, it } from "node:test";
import assert from "node:assert/strict";

import { pollOnce, type EventsPage, type LedgerReader, type PageSink, type PollAlert } from "./poll.js";
import type { DecodedEvent, RpcEvent } from "./types.js";

const CONTRACT = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABSC4";

function rpcEvent(id: string, ledger: number): RpcEvent {
  return {
    id,
    pagingToken: id,
    ledger,
    ledgerClosedAt: `2026-10-17T00:00:0${ledger % 10}Z`,
    contractId: CONTRACT,
    topic: [],
    value: {},
  } as unknown as RpcEvent;
}

function decode(raw: RpcEvent): DecodedEvent {
  return {
    id: raw.id,
    pagingToken: raw.pagingToken ?? raw.id,
    ledger: raw.ledger,
    ledgerClosedAt: raw.ledgerClosedAt,
    contractId: raw.contractId,
    name: "stream_created",
    streamId: "1",
    data: {},
  };
}

/** A ledger that answers with a fixed script of pages, in order. */
function scriptedReader(pages: EventsPage[]): LedgerReader & { calls: number } {
  let i = 0;
  const reader = {
    calls: 0,
    async getEvents(): Promise<EventsPage> {
      reader.calls += 1;
      const page = pages[Math.min(i, pages.length - 1)];
      i += 1;
      return page;
    },
    async getLatestLedger(): Promise<{ sequence: number }> {
      return { sequence: 0 };
    },
  };
  return reader;
}

/** In-memory sink with the same contract as StreamStore's used subset. */
function memorySink(initialCursor?: string) {
  const state = new Map<string, string>();
  if (initialCursor) state.set("cursor", initialCursor);
  const ingested: DecodedEvent[] = [];
  const sink: PageSink & { ingested: DecodedEvent[]; state: Map<string, string> } = {
    ingested,
    state,
    getState(key: string): string | undefined {
      return state.get(key);
    },
    ingestPage(events: DecodedEvent[], cursor: string | undefined, latestLedger: number): void {
      ingested.push(...events);
      if (cursor) state.set("cursor", cursor);
      state.set("lastLedger", String(latestLedger));
    },
  };
  return sink;
}

const OPTIONS = { filters: [{ type: "contract" }], decode };

describe("pollOnce — chaos against a misbehaving ledger", () => {
  it("ingests a normal page and stores the cursor", async () => {
    const reader = scriptedReader([
      { events: [rpcEvent("a", 5), rpcEvent("b", 6)], cursor: "cur-1", latestLedger: 10 },
    ]);
    const sink = memorySink();
    const result = await pollOnce(reader, sink, { ...OPTIONS, startLedger: 1 });

    assert.equal(result.ingested, 2);
    assert.equal(sink.getState("cursor"), "cur-1");
    assert.equal(sink.getState("lastLedger"), "10");
  });

  it("stops instead of spinning when the cursor rewinds", async () => {
    // The node keeps handing back the same cursor with more events: without the
    // guard this loop would page until maxPages and re-ingest the same stream.
    const stuck: EventsPage = {
      events: [rpcEvent("a", 5)],
      cursor: "cur-1",
      latestLedger: 10,
    };
    const reader = scriptedReader([stuck, stuck, stuck, stuck, stuck, stuck]);
    const sink = memorySink();
    const alerts: PollAlert[] = [];
    const result = await pollOnce(reader, sink, {
      ...OPTIONS,
      startLedger: 1,
      pageLimit: 1,
      onAlert: (a) => alerts.push(a),
    });

    assert.ok(alerts.some((a) => a.kind === "duplicate_page"), "rewind must raise an alert");
    assert.ok(result.pages <= 3, `loop stopped early, paged ${result.pages} times`);
    assert.equal(result.ingested, 1, "the repeated event is not ingested twice");
  });

  it("de-duplicates an event that reappears in a later page", async () => {
    const reader = scriptedReader([
      { events: [rpcEvent("a", 5)], cursor: "cur-1", latestLedger: 10 },
      { events: [rpcEvent("a", 5), rpcEvent("b", 6)], cursor: "cur-2", latestLedger: 11 },
    ]);
    const sink = memorySink();
    const result = await pollOnce(reader, sink, { ...OPTIONS, startLedger: 1, pageLimit: 1 });

    assert.deepEqual(sink.ingested.map((e) => e.id), ["a", "b"]);
    assert.equal(result.duplicates, 1);
  });

  it("does not rewind stored state when the replica falls behind", async () => {
    const reader = scriptedReader([
      { events: [rpcEvent("a", 9)], cursor: "cur-9", latestLedger: 20 },
      { events: [rpcEvent("b", 10)], cursor: "cur-10", latestLedger: 15 },
    ]);
    const sink = memorySink();
    const alerts: PollAlert[] = [];
    await pollOnce(reader, sink, {
      ...OPTIONS,
      startLedger: 1,
      pageLimit: 1,
      onAlert: (a) => alerts.push(a),
    });

    assert.ok(alerts.some((a) => a.kind === "rpc_divergence"), "stale latestLedger is reported");
    assert.equal(sink.getState("lastLedger"), "20", "highest ledger wins, never the lower one");
    assert.equal(sink.getState("cursor"), "cur-10", "cursor still advances to the newest page");
  });

  it("skips an event whose ledger goes backwards and flags the gap", async () => {
    const reader = scriptedReader([
      { events: [rpcEvent("a", 20), rpcEvent("late", 7)], cursor: "cur-1", latestLedger: 25 },
    ]);
    const sink = memorySink();
    const alerts: PollAlert[] = [];
    await pollOnce(reader, sink, { ...OPTIONS, startLedger: 1, onAlert: (a) => alerts.push(a) });

    assert.deepEqual(sink.ingested.map((e) => e.id), ["a"], "the old event is not ingested late");
    assert.ok(alerts.some((a) => a.kind === "ledger_gap"));
  });

  it("caps one tick so a pathological node cannot page forever", async () => {
    const endless = (n: number): EventsPage => ({
      events: [rpcEvent(`e${n}`, n + 1)],
      cursor: `cur-${n}`,
      latestLedger: n + 100,
    });
    const pages = Array.from({ length: 20 }, (_, n) => endless(n));
    const reader = scriptedReader(pages);
    const sink = memorySink();
    const result = await pollOnce(reader, sink, { ...OPTIONS, startLedger: 1, pageLimit: 1, maxPages: 5 });

    assert.equal(result.pages, 5);
    assert.equal(result.ingested, 5);
  });
});
