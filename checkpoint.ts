/**
 * Provides an implementation of Langchain's Checkpoint saver interface, storing
 * checkpoints in a Deno.Kv store.
 *
 * @module
 */

import { batchedAtomic } from "@kitsonk/kv-toolbox/batched_atomic";
import * as blob from "@kitsonk/kv-toolbox/blob";
import { query } from "@kitsonk/kv-toolbox/query";
import type { RunnableConfig } from "@langchain/core/runnables";
import {
  BaseCheckpointSaver,
  type ChannelVersions,
  type Checkpoint,
  type CheckpointListOptions,
  type CheckpointMetadata,
  type CheckpointPendingWrite,
  type CheckpointTuple,
  type PendingWrite,
  type SerializerProtocol,
} from "@langchain/langgraph-checkpoint";

/**
 * Options for the {@linkcode DenoKvSaver}.
 */
export interface DenoKvSaverParams {
  /**
   * `Deno.Kv` instance or path to `Deno.Kv` store. Defaults to the local
   * instance.
   */
  store?: Deno.Kv | string;
  /**
   * Prefix for checkpoint keys in the store, defaults to
   * `["__langchain_checkpoint__"]`
   */
  prefix?: Deno.KvKey;
  /**
   * Prefix for checkpoint pending writes keys in the store, defaults to
   * `["__langchain_checkpoint_writes__"]`
   */
  writesPrefix?: Deno.KvKey;
  /**
   * Optionally an `expireIn` option can be specified to set a time-to-live
   * (TTL) for the key. The TTL is specified in milliseconds, and the key will
   * be deleted from the database at earliest after the specified number of
   * milliseconds have elapsed. Once the specified duration has passed, the key
   * may still be visible for some additional time. If the `expireIn` option is
   * not specified, the key will not expire.
   */
  expireIn?: number;
}

interface StoredCheckpoint {
  thread_id: string;
  checkpoint_ns: string;
  checkpoint_id: string;
  parent_checkpoint_id: string;
  type: string;
}

interface StoredWrite {
  taskId: string;
  channel: string;
  type: string;
}

const DEFAULT_PREFIX: Deno.KvKey = ["__langchain_checkpoint__"];
const DEFAULT_WRITES_PREFIX: Deno.KvKey = ["__langchain_checkpoint_writes__"];
const CHECKPOINT_KEYPART: Deno.KvKeyPart = "checkpoint";
const METADATA_KEYPART: Deno.KvKeyPart = "metadata";
const VALUE_KEYPART: Deno.KvKeyPart = "value";

/**
 * A checkpoint saver implementation using {@linkcode Deno.Kv}.
 *
 * In addition to the standard checkpointing functionality, this implementation
 * also provides the `.end()` method to close the underlying `Deno.Kv` store.
 *
 * @example
 *
 * ```ts
 * import { DenoKvSaver } from "@kitsonk/langchain-kv/checkpoint";
 * import { createReactAgent } from "npm:@langchain/langgraph/prebuilt";
 * import { ChatDeepSeek } from "npm:@langchain/deepseek";
 *
 * const llm = new ChatDeepSeek({
 *   model: "deepseek-chat",
 * });
 *
 * const agent = createReactAgent({
 *   // @ts-ignore assignment issues
 *   llm,
 *   tools: [],
 *   checkpoint: new DenoKvSaver({ store: ":memory:" })
 * });
 * ```
 */
export class DenoKvSaver extends BaseCheckpointSaver {
  #prefix: Deno.KvKey;
  #storePromise: Promise<Deno.Kv>;
  #writesPrefix: Deno.KvKey;
  #expireIn?: number;

  async #getPendingWrites(
    thread_id: Deno.KvKeyPart,
    checkpoint_ns: Deno.KvKeyPart,
    checkpoint_id: Deno.KvKeyPart,
  ): Promise<CheckpointPendingWrite[]> {
    const store = await this.#storePromise;
    const writesPrefix: Deno.KvKey = [
      ...this.#writesPrefix,
      thread_id,
      checkpoint_ns,
      checkpoint_id,
    ];
    const pendingWrites: CheckpointPendingWrite[] = [];
    for await (
      const { key, value } of store.list<StoredWrite>({ prefix: writesPrefix })
    ) {
      const maybeSerializedValue = await blob.get(store, [
        ...key,
        VALUE_KEYPART,
      ]);
      if (!maybeSerializedValue.value) {
        continue;
      }
      const serializedValue = maybeSerializedValue.value;
      pendingWrites.push([
        value.taskId,
        value.channel,
        await this.serde.loadsTyped(value.type, serializedValue),
      ]);
    }
    return pendingWrites;
  }

  async #getStoredCheckpointEntry(
    threadId: Deno.KvKeyPart,
    checkpointNs: Deno.KvKeyPart,
    checkpointId?: Deno.KvKeyPart,
  ): Promise<Deno.KvEntry<StoredCheckpoint> | undefined> {
    const store = await this.#storePromise;
    if (checkpointId) {
      const key = [...this.#prefix, threadId, checkpointNs, checkpointId];
      const maybeEntry = await store.get<StoredCheckpoint>(key);
      if (maybeEntry.value) {
        return maybeEntry;
      } else {
        return undefined;
      }
    }
    const prefix = [...this.#prefix, threadId, checkpointNs];
    const list = store.list<StoredCheckpoint>({ prefix }, { reverse: true });
    for await (const entry of list) {
      if (entry.key.length === prefix.length + 1) {
        return entry;
      }
    }
    return undefined;
  }

  constructor(params: DenoKvSaverParams = {}, serde?: SerializerProtocol) {
    super(serde);
    const {
      store,
      prefix = DEFAULT_PREFIX,
      writesPrefix = DEFAULT_WRITES_PREFIX,
    } = params;
    this.#storePromise = (!store || typeof store === "string") ? Deno.openKv(store) : Promise.resolve(store);
    this.#prefix = prefix;
    this.#writesPrefix = writesPrefix;
    this.#expireIn = params.expireIn;
  }

  /**
   * Get the checkpoint tuple for the given config.
   */
  async getTuple(config: RunnableConfig): Promise<CheckpointTuple | undefined> {
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};
    if (!thread_id) {
      return undefined;
    }
    const entry = await this.#getStoredCheckpointEntry(
      thread_id,
      checkpoint_ns,
      checkpoint_id,
    );
    if (!entry) {
      return undefined;
    }
    const doc = entry.value;
    const store = await this.#storePromise;
    const maybeSerializedCheckpoint = await blob.get(store, [
      ...entry.key,
      CHECKPOINT_KEYPART,
    ]);
    const maybeSerializedMetadata = await blob.get(store, [
      ...entry.key,
      METADATA_KEYPART,
    ]);
    if (!maybeSerializedCheckpoint.value || !maybeSerializedMetadata.value) {
      return undefined;
    }
    const serializedCheckpoint = maybeSerializedCheckpoint.value;
    const serializedMetadata = maybeSerializedMetadata.value;
    const configurable = {
      thread_id,
      checkpoint_ns,
      checkpoint_id: doc.checkpoint_id,
    };
    const checkpoint = (await this.serde.loadsTyped(
      doc.type,
      serializedCheckpoint,
    )) as Checkpoint;
    const pendingWrites = await this.#getPendingWrites(
      thread_id,
      checkpoint_ns,
      doc.checkpoint_id,
    );
    const parentConfig = doc.parent_checkpoint_id != null
      ? {
        configurable: {
          thread_id,
          checkpoint_ns,
          checkpoint_id: doc.parent_checkpoint_id,
        },
      }
      : undefined;
    return {
      config: { configurable },
      checkpoint,
      pendingWrites,
      metadata: (await this.serde.loadsTyped(doc.type, serializedMetadata)),
      parentConfig,
    };
  }

  /**
   * List all checkpoints in the store filtered by the given options.
   */
  async *list(config: RunnableConfig, options: CheckpointListOptions = {}): AsyncGenerator<CheckpointTuple> {
    const { limit, before, filter } = options;
    const prefix = [...this.#prefix];
    if (config.configurable?.thread_id) {
      prefix.push(config.configurable.thread_id);
      if (config.configurable.checkpoint_ns != null) {
        prefix.push(config.configurable.checkpoint_ns);
      }
    }
    const store = await this.#storePromise;
    const q = query<StoredCheckpoint>(store, { prefix }, { reverse: true });
    if (before) {
      q.where("checkpoint_id", "<", before.configurable?.checkpoint_id);
    }
    let count = 0;
    for await (const { key, value } of q.get()) {
      if (
        config.configurable?.checkpoint_ns != null &&
        value.checkpoint_ns !== config.configurable.checkpoint_ns
      ) {
        continue;
      }
      const maybeSerializedMetadata = await blob.get(store, [
        ...key,
        METADATA_KEYPART,
      ]);
      if (!maybeSerializedMetadata.value) {
        continue;
      }
      const serializedMetadata = maybeSerializedMetadata.value;
      const metadata: CheckpointMetadata = await this.serde.loadsTyped(
        value.type,
        serializedMetadata,
      );
      if (filter) {
        let match = true;
        for (const [key, val] of Object.entries(filter)) {
          if (metadata[key as keyof CheckpointMetadata] !== val) {
            match = false;
            break;
          }
        }
        if (!match) {
          continue;
        }
      }
      const maybeSerializedCheckpoint = await blob.get(store, [
        ...key,
        CHECKPOINT_KEYPART,
      ]);
      if (!maybeSerializedCheckpoint.value) {
        continue;
      }
      const serializedCheckpoint = maybeSerializedCheckpoint.value;
      const checkpoint: Checkpoint = await this.serde.loadsTyped(
        value.type,
        serializedCheckpoint,
      );
      const pendingWrites = await this.#getPendingWrites(
        value.thread_id,
        value.checkpoint_ns,
        value.checkpoint_id,
      );
      const parentConfig = value.parent_checkpoint_id
        ? {
          configurable: {
            thread_id: value.thread_id,
            checkpoint_ns: value.checkpoint_ns,
            checkpoint_id: value.parent_checkpoint_id,
          },
        }
        : undefined;
      count++;
      yield {
        config: {
          configurable: {
            thread_id: value.thread_id,
            checkpoint_ns: value.checkpoint_ns,
            checkpoint_id: value.checkpoint_id,
          },
        },
        checkpoint,
        metadata,
        pendingWrites,
        parentConfig,
      };
      if (limit && count >= limit) {
        break;
      }
    }
  }

  /**
   * Store a checkpoint.
   *
   * @TODO This method should be updated to handle the `newVersions` parameter
   */
  async put(
    config: RunnableConfig,
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    _newVersions: ChannelVersions,
  ): Promise<RunnableConfig> {
    const {
      thread_id,
      checkpoint_ns = "",
    } = config.configurable ?? {};
    const checkpoint_id = checkpoint.id;
    if (!thread_id) {
      throw new Error(
        `The provided config must contain a configurable field with a "thread_id" field.`,
      );
    }

    // If newVersions are provided, only persist channel_values that correspond
    // to channels whose versions changed. This mirrors the behavior in the
    // Postgres implementation and satisfies the validator expectations.
    const newVersionKeys = Object.keys(_newVersions ?? {});
    const checkpointToStore: Checkpoint = {
      ...checkpoint,
      channel_values: Object.fromEntries(
        Object.entries(checkpoint.channel_values ?? {}).filter(([key]) => newVersionKeys.includes(key)),
      ),
    };

    const [
      checkpointType,
      serializedCheckpoint,
    ] = await this.serde.dumpsTyped(checkpointToStore);
    const [metadataType, serializedMetadata] = await this.serde.dumpsTyped(metadata);
    if (checkpointType !== metadataType) {
      throw new Error("Mismatched types for checkpoint and metadata");
    }
    const value = {
      thread_id,
      checkpoint_ns,
      checkpoint_id,
      parent_checkpoint_id: config.configurable?.checkpoint_id,
      type: checkpointType,
    } satisfies StoredCheckpoint;
    const store = await this.#storePromise;
    const key = [...this.#prefix, thread_id, checkpoint_ns, checkpoint_id];
    const res = await batchedAtomic(store)
      .set(key, value, { expireIn: this.#expireIn })
      .setBlob([...key, CHECKPOINT_KEYPART], serializedCheckpoint, {
        expireIn: this.#expireIn,
      })
      .setBlob([...key, METADATA_KEYPART], serializedMetadata, {
        expireIn: this.#expireIn,
      })
      .commit();
    if (!res.every((r) => r.ok)) {
      throw new Error(`Failed to put checkpoint ${checkpoint_id}`);
    }
    return {
      configurable: {
        thread_id,
        checkpoint_ns,
        checkpoint_id,
      },
    };
  }

  /**
   * Store pending writes for a checkpoint.
   */
  async putWrites(config: RunnableConfig, writes: PendingWrite[], taskId: string): Promise<void> {
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};
    if (thread_id == null || checkpoint_id == null) {
      throw new Error(
        `The provided config must contain a configurable field with a "thread_id" and "checkpoint_id" field.`,
      );
    }
    const store = await this.#storePromise;
    const transaction = batchedAtomic(store);
    for (const [idx, [channel, value]] of Object.entries(writes)) {
      const [type, serializedValue] = await this.serde.dumpsTyped(value);
      const key = [
        ...this.#writesPrefix,
        thread_id,
        checkpoint_ns,
        checkpoint_id,
        taskId,
        idx,
      ];
      const writeValue: StoredWrite = { taskId, channel, type };
      transaction
        .set(key, writeValue, { expireIn: this.#expireIn })
        .setBlob(
          [...key, VALUE_KEYPART],
          serializedValue,
          { expireIn: this.#expireIn },
        );
    }
    await transaction.commit();
  }

  /**
   * Delete all checkpoints and writes associated with a specific thread ID.
   */
  async deleteThread(threadId: string): Promise<void> {
    const store = await this.#storePromise;
    const transaction = batchedAtomic(store);
    const prefix = [...this.#prefix, threadId];
    for await (const { key } of store.list({ prefix })) {
      transaction.delete(key);
    }
    const writesPrefix = [...this.#writesPrefix, threadId];
    for await (const { key } of store.list({ prefix: writesPrefix })) {
      transaction.delete(key);
    }
    await transaction.commit();
  }

  /**
   * Closes the underlying `Deno.Kv` store.
   */
  async end(): Promise<void> {
    (await this.#storePromise).close();
  }
}
