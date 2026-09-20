## Summary

<!-- What changed and why. Link the issue this closes, if any. -->

## Type of change

- [ ] Bug fix (no behaviour change for correct inputs)
- [ ] New feature (stage / source / sink / binding / CLI option)
- [ ] Refactor or internal cleanup
- [ ] Documentation, examples, or CI only
- [ ] Breaking change (call out the migration path below)

## Checklist

- [ ] `cargo fmt --all --check` passes
- [ ] `cargo clippy --workspace --all-features -- -D warnings` passes
- [ ] `cargo test --workspace --all-features` passes
- [ ] `cargo deny check licenses bans sources` passes (no new Apache-2.0-only dependency)
- [ ] If a binding changed: Python (`just pytest`) and/or JS (`just jstest`) tests pass
- [ ] `CHANGELOG.md` updated, plus the affected crate's own `CHANGELOG.md` and `README.md`
- [ ] Docs updated where behaviour or CLI/config syntax changed (`README.md`, `tpt-stream-cli/README.md`, crate `README.md`)
- [ ] New sources/sinks/stages are feature-gated so the wasm `default-features = false` build still compiles

## Testing

<!-- The commands you ran and what they showed. -->

```sh
just ci          # fmt + clippy + deny + test
```

## Notes for reviewers

<!-- Trade-offs, follow-ups, or anything intentionally left out. -->
