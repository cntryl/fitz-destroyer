# Fitz Destroyer Agent Guide

## Purpose

This repository is a disposable, local-only adversarial harness for Fitz. It is
not a benchmark suite, production deployable, or a source of Fitz semantics.

## Constraints

- Keep orchestration on the host. Never mount the Docker socket into a container.
- Use Sqrzl's S3 emulator; do not require AWS credentials, an AWS account, or MinIO.
- Bind published ports to loopback only.
- Give every run a unique Compose project and storage prefix.
- Remove only Compose resources resolved from that exact project.
- Preserve local failure artifacts under `artifacts/`.
- Do not add CI unless explicitly requested.
- Use `should_*` names for tests.

## Validation

Run `npm run check`. When Docker is available, also run an explicit smoke
scenario such as `npm run destroy -- clean-restart --scale smoke`.
