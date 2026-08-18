# AGENTS.md - Engineering Constraints

## Core Principles

1. **Do not preserve backward compatibility.** Remove obsolete paths aggressively.
2. **Choose the simplest implementation** that satisfies requirements.
3. **Keep components modular** and concerns clearly separated.
4. **Grow the system in layers.** Start from the smallest version that works end to end.
5. **Prefer established, well-maintained libraries.** Lean on dependencies already in the project.
6. **Make architectural decisions for the long term.** Do not accept a stopgap.

## Agent Workflow

- Each agent owns a vertical slice of functionality.
- Agents reference `brain/` for strategy and SOPs before implementing.
- Cross-verification between agents is mandatory for security-critical paths.
- All outputs must pass the Eval Loop before merging.

## Code Quality Gates

- No hardcoded secrets or credentials.
- All PII must be tokenized before storage or logging.
- All data transformations must have audit trails.
- All stream processing must handle late arrivals and out-of-order events.
- All aggregations must be idempotent.

## Documentation

- Every module must have inline docstrings.
- Architecture decisions documented in `docs/`.
- Lessons learned captured in `tasks/lessons.md`.
