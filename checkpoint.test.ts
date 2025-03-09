import { assertEquals } from "jsr:@std/assert@~1/equals";
import { DenoKvSaver } from "./checkpoint.ts";

Deno.test({
  name: "DenoKvSaver",
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
