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

interface DenoKvSaverParams {
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
  checkpoint: Uint8Array;
  metadata: Uint8Array;
}

interface StoredWrite {
  taskId: string;
  channel: string;
  type: string;
  value: Uint8Array;
}

export class DenoKvSaver extends BaseCheckpointSaver {
  #prefix: Deno.KvKey;
  #storePromise: Promise<Deno.Kv>;
  #writesPrefix: Deno.KvKey;
  #expireIn?: number;

  constructor(params: DenoKvSaverParams = {}, serde?: SerializerProtocol) {
    super(serde);
    const {
      store,
      prefix = ["__langchain_checkpoint__"],
      writesPrefix = ["__langchain_checkpoint_writes__"],
    } = params;
    this.#storePromise = (!store || typeof store === "string")
      ? Deno.openKv(store)
      : Promise.resolve(store);
    this.#prefix = prefix;
    this.#writesPrefix = writesPrefix;
    this.#expireIn = params.expireIn;
  }

  override async getTuple(
    config: RunnableConfig,
  ): Promise<CheckpointTuple | undefined> {
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};
    if (!thread_id) {
      return undefined;
    }
    const prefix = checkpoint_id
      ? [...this.#prefix, thread_id, checkpoint_ns, checkpoint_id]
      : [...this.#prefix, thread_id, checkpoint_ns];
    const store = await this.#storePromise;
    const iterator = store.list<StoredCheckpoint>({ prefix }, {
      limit: 1,
      reverse: true,
    });
    const { value } = await iterator.next();
    if (!value) {
      return undefined;
    }
    const doc = value.value;
    const configurable = {
      thread_id,
      checkpoint_ns,
      checkpoint_id: doc.checkpoint_id,
    };
    const checkpoint =
      (await this.serde.loadsTyped(doc.type, doc.checkpoint)) as Checkpoint;
    const writesPrefix = [
      ...this.#writesPrefix,
      thread_id,
      checkpoint_ns,
      doc.checkpoint_id,
    ];
    const pendingWrites: CheckpointPendingWrite[] = [];
    for await (
      const { value } of store.list<StoredWrite>({ prefix: writesPrefix })
    ) {
      pendingWrites.push([
        value.taskId,
        value.channel,
        this.serde.loadsTyped(value.type, value.value),
      ]);
    }
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
      metadata: (await this.serde.loadsTyped(doc.type, doc.metadata)),
      parentConfig,
    };
  }

  override async *list(
    config: RunnableConfig,
    options: CheckpointListOptions = {},
  ): AsyncGenerator<CheckpointTuple> {
    const { limit, before, filter } = options;
    const prefix = [...this.#prefix];
    if (config.configurable?.thread_id) {
      prefix.push(config.configurable.thread_id);
      if (config.configurable.checkpoint_ns) {
        prefix.push(config.configurable.checkpoint_ns);
      }
    }
    const store = await this.#storePromise;
    const q = query<StoredCheckpoint>(store, { prefix }, { reverse: true });
    if (before) {
      q.where("checkpoint_id", "<", before.configurable?.checkpoint_id);
    }
    let count = 0;
    for await (const { value } of q.get()) {
      const metadata: CheckpointMetadata = await this.serde.loadsTyped(
        value.type,
        value.metadata,
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
      const checkpoint: Checkpoint = await this.serde.loadsTyped(
        value.type,
        value.checkpoint,
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
        parentConfig,
      };
      if (limit && count >= limit) {
        break;
      }
    }
  }

  override async put(
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
    const [
      checkpointType,
      serializedCheckpoint,
    ] = this.serde.dumpsTyped(checkpoint);
    const [metadataType, serializedMetadata] = this.serde.dumpsTyped(metadata);
    if (checkpointType !== metadataType) {
      throw new Error("Mismatched types for checkpoint and metadata");
    }
    const value = {
      thread_id,
      checkpoint_ns,
      checkpoint_id,
      parent_checkpoint_id: config.configurable?.checkpoint_id,
      type: checkpointType,
      checkpoint: serializedCheckpoint,
      metadata: serializedMetadata,
    } satisfies StoredCheckpoint;
    const store = await this.#storePromise;
    const res = await store.set(
      [...this.#prefix, thread_id, checkpoint_ns, checkpoint_id],
      value,
      { expireIn: this.#expireIn },
    );
    if (!res.ok) {
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

  override async putWrites(
    config: RunnableConfig,
    writes: PendingWrite[],
    taskId: string,
  ): Promise<void> {
    const {
      thread_id,
      checkpoint_ns = "",
      checkpoint_id,
    } = config.configurable ?? {};
    if (!thread_id || !checkpoint_id) {
      throw new Error(
        `The provided config must contain a configurable field with a "thread_id" or "checkpoint_id" field.`,
      );
    }
    const store = await this.#storePromise;
    const transaction = store.atomic();
    for (const [idx, [channel, value]] of Object.entries(writes)) {
      const [type, serializedValue] = this.serde.dumpsTyped(value);
      const key = [
        ...this.#writesPrefix,
        thread_id,
        checkpoint_ns,
        checkpoint_id,
        taskId,
        idx,
      ];
      transaction.set(key, {
        taskId,
        channel,
        type,
        value: serializedValue,
      }, { expireIn: this.#expireIn });
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
