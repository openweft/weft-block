# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **End-to-end snapshot/backup driver pipeline** for the `weft-block` plugin :
  - Snapshot CRUD + Revert through the standard `drivers.VolumeDriver` interface (`CreateSnapshot` / `ListSnapshots` / `DeleteSnapshot` / `RevertSnapshot`).
  - `CreateBackup` / `ListBackups` / `DeleteBackup` / `RestoreBackup` honouring encrypted + incremental chains.
  - Driver-side dispatch on `BackupSpec.ParentURL` : when set, `weft-block` calls the replica's `weftsnap.Compare` RPC to derive the byte windows that differ, then streams only those ranges. Falls back gracefully to a full backup when the replica returns `Unimplemented` (the receiver logs the fall-back with a clear note).
  - Sparse stream support : `ReadRequest.Ranges` on `weftsnap.Read` walks the ranges in order and emits monotonically-increasing offsets.
- **AEAD encrypted backup pipeline** (`pkg/backupcrypto`) :
  - ChaCha20-Poly1305 (default) + AES-256-GCM.
  - Argon2id KDF (OWASP defaults : 64 MiB / 3 iters / 2 parallelism) + `raw` mode for KMS-managed 256-bit keys.
  - Per-chunk nonces (8-byte random stream prefix + 4-byte monotonic counter) with the 32-byte stream header used as AAD — tampering with chunk size or algorithm-id invalidates every chunk's tag.
  - Salt + algo + KDF params stored alongside the backup metadata ; the passphrase is loaded by the daemon from `WEFT_BACKUP_PASSPHRASE` (override env via `WEFT_BACKUP_PASSPHRASE_ENV`) and never crosses the wire.
- **Four `BackupTarget` schemes** (`pkg/backuptarget`) :
  - `oci://<registry>/<repo>:<tag>` (default, content-addressed, cosign-signable) via `oras-go` v2 ; artifact type `application/vnd.openweft.weft-block.backup.v1`.
  - `s3://<bucket>@<region>/<prefix>` for versitygw / CubeFS objectnode (MinIO deliberately excluded per the openweft no-AGPL policy).
  - `sftp://<user>@<host>:<port>/<path>` for sftpgo / OpenSSH sshd, with host-key verification on by default.
  - `fs:///<absolute_path>` for dev / tests (atomic rename through `.tmp`, recursive walk, idempotent delete).
- **Public `Replica.CompareSnapshot`** (`pkg/replica/replica.go`) — lifecycle-free analogue of `BackupStatus.CompareSnapshot`. Walks the sector→file location table, aligns the diff to a caller-chosen `blockSize`, emits one `SnapshotRange` per touched block window. Backs the `weftsnap.Compare` RPC.
- **`weftsnap.Compare` RPC implementation** (`pkg/replica/rpc/weftsnap_server.go`) — replaces the previous `codes.Unimplemented` stub. Opens the snapshot via `NewReadOnly`, calls `Replica.CompareSnapshot`, translates `[]SnapshotRange` to the wire format. The driver-side `grpcCodeUnimplemented` detector + graceful full-backup fallback stays in place to handle older replicas.
- **`pkg/backuptarget` integration tests** : 11 sub-tests covering Push/Pull round-trip across five sizes, atomic rewrite, recursive List, idempotent Delete, URL validation, encrypted round-trip (both algorithms) with on-disk ciphertext verification, and wrong-key rejection.
- **`pkg/replica` Linux-tagged tests** : `Replica.CompareSnapshot` correctness against a synthetic chain (empty parent, single-sector diff, multi-sector diff, error cases).

