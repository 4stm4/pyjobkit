# Security policy

## Supported versions

Pyjobkit currently ships its first stable line (`1.x`). Security fixes
are released against the latest `1.x` minor. Older `0.x` releases are
not supported.

| Version | Supported |
| --- | --- |
| 1.x | Yes |
| 0.x | No |

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security reports.

Email **security@4stm4.dev** with a description of the issue, the
affected version(s), and ideally a reproducer or proof-of-concept. We
will:

1. Acknowledge receipt within three business days.
2. Investigate and confirm the vulnerability.
3. Prepare a fix and a coordinated release.
4. Credit the reporter in the release notes unless asked otherwise.

If you do not receive a response within a week, please re-send via the
same address.

## Known sharp edges

A few features ship with explicit security warnings in their docstrings;
treat them as off-by-default if your `enqueue` surface is exposed to
untrusted producers:

- `SubprocessExecutor` runs the command from the payload. Pass
  `allowed_commands=[...]` to scope what it can launch.
- `DockerExecutor` runs containers configured from the payload (image,
  command, env). Run it on dedicated workers with no privileged daemon
  access if you expose enqueue externally.
- The REST integration (`pyjobkit.integrations.fastapi.make_router`) is
  unauthenticated by default. Pass `dependencies=[Depends(...)]` to wire
  authentication into every endpoint.
- Webhook deliveries are unsigned unless `PYJOBKIT_WEBHOOK_SECRET` is
  set; receivers should verify the `X-Pyjobkit-Signature` header.

These are documented in `docs/configuration.md` and the relevant
module docstrings.
