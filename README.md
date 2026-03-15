# Kanoniv

<p align="center">
  <strong>The identity and delegation layer for AI agents.</strong>
</p>

<p align="center">
  <a href="https://crates.io/crates/kanoniv-agent-auth"><img src="https://img.shields.io/crates/v/kanoniv-agent-auth" alt="crates.io"></a>
  <a href="https://www.npmjs.com/package/@kanoniv/agent-auth"><img src="https://img.shields.io/npm/v/@kanoniv/agent-auth" alt="npm"></a>
  <a href="https://pypi.org/project/kanoniv-agent-auth/"><img src="https://img.shields.io/pypi/v/kanoniv-agent-auth" alt="PyPI"></a>
  <a href="https://opensource.org/licenses/MIT"><img src="https://img.shields.io/badge/license-MIT-blue.svg" alt="License"></a>
</p>

---

Kanoniv gives every AI agent a cryptographic identity (`did:agent:`) and verifiable delegation chains. Agents carry proof of who they are and what they are authorized to do. Any MCP server verifies the proof in 5 lines. No external lookups needed.

## Architecture

```
Layer 0: Identity           did:agent: DIDs              (open source)
Layer 1: Delegation         attenuated authority chains   (open source)
Layer 2: MCP Auth           proofs on tool calls          (open source)
Layer 3: Memory             entity-linked knowledge       (Cloud)
Layer 4: Agent Resolution   same agent across platforms   (Cloud)
Layer 5: Audit              signed provenance trail       (Cloud)
```

Layers 0-2 are free and open source (MIT). Layers 3-5 require [Kanoniv Cloud](https://kanoniv.com/pricing/).

## Repositories

| Repo | Description |
|------|-------------|
| [**agent-auth**](https://github.com/kanoniv/agent-auth) | Core library: Ed25519 identity, delegation, MCP proofs. Rust + TypeScript + Python. |
| [**kanoniv-crewai**](https://github.com/kanoniv/kanoniv-crewai) | CrewAI integration: `DelegatedCrew`, `delegated_tool` |
| [**kanoniv-langgraph**](https://github.com/kanoniv/kanoniv-langgraph) | LangGraph integration: `DelegatedGraph`, `@delegated_node` |
| [**kanoniv-autogen**](https://github.com/kanoniv/kanoniv-autogen) | AutoGen integration: `AuthorityManager`, `DelegatedAgent` |
| [**kanoniv-openai-agents**](https://github.com/kanoniv/kanoniv-openai-agents) | OpenAI Agents SDK integration: `DelegatedRunner`, `@delegated_tool` |

## Install

```bash
# Core library
cargo add kanoniv-agent-auth        # Rust
npm install @kanoniv/agent-auth     # TypeScript
pip install kanoniv-agent-auth      # Python

# CLI
npx @kanoniv/agent-auth generate
npx @kanoniv/agent-auth delegate --to did:agent:... --scope resolve,search --max-cost 5
npx @kanoniv/agent-auth verify --proof proof.json --root did:agent:...

# Framework integrations
pip install kanoniv-crewai
pip install kanoniv-langgraph
pip install kanoniv-autogen
pip install kanoniv-openai-agents
```

## Quick Start (5 lines to add auth to any MCP server)

```typescript
import { McpProof, verifyMcpCall } from "@kanoniv/agent-auth";

function handleToolCall(args) {
  const { proof, cleanArgs } = McpProof.extract(args);
  if (proof) {
    const result = verifyMcpCall(proof, rootIdentity);
    console.log(`Agent ${result.invoker_did} verified (depth: ${result.depth})`);
  }
}
```

## How Delegation Works

```
Root Authority (Human)
  |-- delegates to Manager: [search, resolve, merge], max $50, expires 24h
      |-- delegates to Worker: [search], max $10
          |-- calls MCP tool with proof
              |-- server verifies chain, checks caveats, executes or rejects
```

Authority narrows at each step. Caveats accumulate. A sub-agent can never exceed its parent's scope.

## Documentation

- [Getting Started](https://kanoniv.com/docs/getting-started/)
- [Identity (Layer 0)](https://kanoniv.com/docs/identity/)
- [Delegation (Layer 1)](https://kanoniv.com/docs/delegation/)
- [MCP Auth (Layer 2)](https://kanoniv.com/docs/mcp/)
- [Memory (Layer 3)](https://kanoniv.com/docs/memory/)
- [Agent Resolution (Layer 4)](https://kanoniv.com/docs/agent-resolution/)
- [Security Model](https://kanoniv.com/docs/security/)
- [SDK References](https://kanoniv.com/docs/sdks/rust)

## MCP Spec Proposal

We proposed adding an `auth` field to MCP tool calls for agent delegation proofs: [Discussion #2404](https://github.com/modelcontextprotocol/modelcontextprotocol/discussions/2404)

## License

MIT

---

<p align="center">
  <a href="https://kanoniv.com">Website</a> &middot;
  <a href="https://kanoniv.com/docs/">Docs</a> &middot;
  <a href="https://github.com/kanoniv/agent-auth">Core Library</a> &middot;
  <a href="https://kanoniv.com/pricing/">Pricing</a>
</p>
