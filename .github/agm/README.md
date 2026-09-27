# Maintained tiered headless build

The `tiered-release.yml` workflow polls the latest stable release from `lbjlaq/Antigravity-Manager`. It checks out that exact upstream commit, applies `tiered-effort.patch`, runs Rust formatting and Clippy checks plus the two focused Rust tests, and builds the Linux x86-64 headless binary. A successful build updates `custom/tiered-effort` and publishes a release tagged `tiered-vX.Y.Z-<patch SHA prefix>` with the binary and the proxy-sg manifest.

The fixed budgets for tiered Flash are low 1000, medium 10000, high 32768, xhigh 49152, and max 65535. The patch also preserves `maxOutputTokens=65536` for max and removes the internal effort marker before upstream I/O.

The workflow stops if an upstream change prevents a clean patch application, either focused test fails, or the source tree published on the maintained branch differs from the source tree compiled by the build job. It never publishes an unpatched upstream binary. Adapt the patch against the new upstream tag, test it, and commit the updated patch to this fork's `main` to retry. A changed patch SHA creates a distinct release tag.

The scheduled workflow must be enabled on this fork in GitHub Actions. Manual `workflow_dispatch` is also available. The release is a build artifact; proxy-sg switches binaries only after its separate idle and manifest checks.
