import "./_test_globals.ts"; // This is needed to provide Jest globals in Deno

import { validate } from "npm:@langchain/langgraph-checkpoint-validation@^0.0.2";
import { assert } from "jsr:@std/assert@~1/assert";
import { assertEquals } from "jsr:@std/assert@~1/equals";

import { DenoKvSaver } from "./checkpoint.ts";

Deno.test({
  name: "DenoKvSaver - put/list",
  async fn() {
    const saver = new DenoKvSaver({ store: ":memory:" });
    await saver.put({
      configurable: { thread_id: "test", checkpoint_id: "test" },
    }, {
      v: 1,
      id: "test",
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    }, {
      source: "input",
      step: -1,
      writes: null,
      parents: {},
    }, {});
    const actual = [];
    for await (
      const checkpoint of saver.list({
        configurable: { thread_id: "test" },
      })
    ) {
      actual.push(checkpoint);
    }
    assertEquals(actual.length, 1);
    await saver.end();
  },
});

Deno.test({
  name: "DenoKvSaver - put/getTuple",
  async fn() {
    const saver = new DenoKvSaver({ store: ":memory:" });
    const config = await saver.put({
      configurable: { thread_id: "test", checkpoint_id: "test" },
    }, {
      v: 1,
      id: "test",
      ts: new Date().toISOString(),
      channel_values: {},
      channel_versions: {},
      versions_seen: {},
      pending_sends: [],
    }, {
      source: "input",
      step: -1,
      writes: null,
      parents: {},
    }, {});
    saver.putWrites(
      config,
      [["__pregel_tasks", "unknown"]],
      "pending_sends_task",
    );
    const actual = await saver.getTuple(config);
    assert(actual);
    console.log(actual);
    await saver.end();
  },
});

// These uses the checkpoint validation library which setups a suite of tests
// that validate the implementation of the Checkpoint interface.

validate({
  checkpointerName: "@kitsonk/langchain-kv/checkpoint/DenoKvSaver",
  createCheckpointer() {
    return new DenoKvSaver({ store: ":memory:" });
  },
  destroyCheckpointer(checkpointer) {
    return checkpointer.end();
  },
});
