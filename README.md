# @kitsonk/langchain-kv

Implementations of various persistence APIs for [LangChain.js](https://js.langchain.com/docs/) on-top of Deno KV.

Current capabilities are:

- `DenoKvSaver` an implementation of the checkpoint saver functionality for LangGraph. This allows persistence and time
  travel of the graph state when working with LangGraph workflows.
- `DenoKvCache` an implementation of the LangChain cache API, which is mainly used to cache results from language
  models.
- `DenoKvChatMessageHistory` an implementation of the LangChain message history API.
- `DenoKvRecordManager` an implementation of the LangChain record manager API.
- `DenoKvByteStore` and implementation of LangChain's byte store API.

---

Copyright 2025 Kitson P. Kelly. All rights reserved. MIT License.
