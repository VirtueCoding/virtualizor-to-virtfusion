# vz2vf-migrate

Migrate KVM virtual machines from Virtualizor to VirtFusion on the same Ceph RBD storage cluster.

## Overview

`vz2vf-migrate` is a Python CLI tool designed for hosting providers migrating their KVM virtual machines from Virtualizor to VirtFusion. It runs on the hypervisor node (Ubuntu 22/24) where both panels manage VMs backed by the same Ceph RBD pool, enabling in-place disk migration without data transfer across the network.

The tool reads VM inventory from the Virtualizor MySQL database, provisions replacement servers via the VirtFusion REST API, copies RBD disk images directly within the Ceph cluster, preserves IPv4 and IPv6 addresses, and optionally rewrites WHMCS billing records so that existing customer services point to VirtFusion. Guest agent injection (qemu-guest-agent and cloud-init) is performed via `virt-customize` before the target VM boots.

Every migration run operates in dry-run mode by default — no mutations are made unless `--execute` is passed. Each VM's progress is recorded in a JSON state file, making the pipeline fully resumable: if a run is interrupted, re-running the same command skips already-completed stages.

## Features

- **Dry-run mode** — default behavior; logs planned actions without making changes
- **Per-VM resumable state** — completed stages are recorded in `migration_state.json` and skipped on re-run
- **Ceph RBD copy** — in-place `rbd cp` within the same pool (up to 3 disks per VM)
- **IPv4 preservation** — primary and additional IPv4 addresses are reassigned via the VirtFusion API
- **IPv6 preservation** — delegated to a PHP helper script on the VirtFusion master node via SSH
- **Guest agent injection** — installs `qemu-guest-agent` (required) and `cloud-init` (best-effort) via `virt-customize`, supporting apt, dnf, yum, zypper, pacman, and apk
- **WHMCS service rewrite** — updates `tblhosting` and `mod_virtfusion_direct` in whichever WHMCS database (A or B) owns the service, wrapped in a single MySQL transaction with row-count verification and automatic rollback on mismatch
- **CSV + JSON reporting** — per-run summary CSV and per-VM JSON artifacts
- **Per-run logging** — all commands logged to `logs/{run_id}/run.log`
- **Single-VPS and batch modes** — migrate one VM, a comma-separated list, or an entire node
- **`--continue-on-error`** — continue processing remaining VMs after a failure
- **`--skip-inject`** — skip guest package injection (useful for MikroTik, RouterOS, Windows)
- **`--skip-shutdown`** — skip shutting down the source VPS before migration

## Architecture

```
vz2vf_migrate/
├── cli.py                  # Argument parsing, orchestration loop, CSV output
├── config.py               # Env file loading + config dataclasses
├── virtualizor.py          # Virtualizor MySQL inventory queries
├── virtfusion.py           # VirtFusion REST API client (create user/server, get/build, add IP)
├── virtfusion_master.py    # SSH client for the PHP IPv6 helper on the VirtFusion master node
├── whmcs.py                # WHMCS MySQL lookup (dual-database A/B fallback)
├── whmcs_rewrite.py        # WHMCS mutation queries (tblhosting + mod_virtfusion_direct)
├── host_ops.py             # Shell command builders for virsh, rbd, virt-customize
├── services.py             # Migration stage orchestration with state persistence
├── migrator.py             # Per-VM pipeline (stage sequencing with resume logic)
├── state.py                # JSON state file persistence and validation
├── inventory.py            # Row normalization (DB rows + libvirt XML → SourceVm)
├── models.py               # Dataclasses (SourceVm, SourceDisk, WhmcsMatch, VmResult)
├── runner.py               # Subprocess execution with dry-run support and password redaction
├── reporting.py            # CSV summary reporter and per-VM JSON artifact writer
└── xml_disks.py            # Libvirt XML parser for RBD disk extraction
```

## Prerequisites

- **Python ≥ 3.10**
- **Virtualizor MySQL** access (the panel's database on the hypervisor node)
- **WHMCS MySQL** access — two databases (A and B) for dual-instance billing setups
- **VirtFusion API** credentials (URL + API token)
- **Ceph client tools** — `rbd` CLI available on the hypervisor
- **`virsh`** — libvirt CLI for domain state checks and shutdown
- **`virt-customize`** — from `libguestfs-tools`, used for guest agent injection
- **SSH access to the VirtFusion master node** — required for IPv6 preservation; the PHP helper script (`scripts/virtfusion_master_ipv6_helper.php`) must be deployed on the master node

## Installation

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
```

This installs the `vz2vf-migrate` console script.

## Configuration

Create a `.env` file in the working directory (or specify a path with `--env-file`). The file uses `KEY=VALUE` syntax, one per line. Lines starting with `#` are comments.

### VirtFusion API

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VF_API_URL` | Yes | — | VirtFusion API base URL |
| `VF_API_TOKEN` | Yes | — | VirtFusion API bearer token |
| `VF_HYPERVISOR_ID` | Yes | — | Target hypervisor ID in VirtFusion |
| `VF_PACKAGE_ID` | No | `1` | VirtFusion package ID for new servers |
| `VF_STORAGE_PROFILE_ID` | No | — | Storage profile override |
| `VF_NETWORK_PROFILE_ID` | No | — | Network profile override |
| `VF_ADDITIONAL_STORAGE1_PROFILE_ID` | No | — | Storage profile for 2nd disk |
| `VF_ADDITIONAL_STORAGE2_PROFILE_ID` | No | — | Storage profile for 3rd disk |
| `VF_OS_TEMPLATE_ID` | Yes | — | OS template ID for server creation |

### Virtualizor Database

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VZ_MYSQL_BIN` | No | `/usr/local/emps/bin/mysql` | Path to the Virtualizor MySQL client binary |
| `VZ_DB_HOST` | Yes | — | Database host |
| `VZ_DB_PORT` | Yes | — | Database port |
| `VZ_DB_NAME` | Yes | — | Database name |
| `VZ_DB_USER` | Yes | — | Database user |
| `VZ_DB_PASS` | Yes | — | Database password |

### WHMCS Databases (A and B)

Both WHMCS database sets follow the same pattern. Replace `{X}` with `A` or `B`:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `WHMCS_{X}_DB_HOST` | Yes | — | Database host |
| `WHMCS_{X}_DB_PORT` | Yes | — | Database port |
| `WHMCS_{X}_DB_NAME` | Yes | — | Database name |
| `WHMCS_{X}_DB_USER` | Yes | — | Database user |
| `WHMCS_{X}_DB_PASS` | Yes | — | Database password |
| `WHMCS_{X}_TARGET_SERVER_ID` | When `--whmcs` | — | Target WHMCS server ID for the rewrite |

### WHMCS Product Map

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `WHMCS_PRODUCT_MAP_FILE` | No | `whmcsmap.conf` | Path to the product map file (relative to cwd or absolute) |
| `WHMCS_MYSQL_BIN` | No | `mysql` | Path to the MySQL client binary for WHMCS queries |

### Ceph

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CEPH_USERNAME` | Yes | — | Ceph client username |
| `CEPH_KEYRING` | No | `/etc/ceph/ceph.client.{CEPH_USERNAME}.keyring` | Path to the Ceph keyring file |

### VirtFusion Master (IPv6)

These variables are required when migrating VMs with IPv6 addresses:

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `VF_MASTER_HOST` | For IPv6 | — | VirtFusion master node hostname or IP |
| `VF_MASTER_PORT` | No | `22` | SSH port |
| `VF_MASTER_USER` | No | `root` | SSH user |
| `VF_MASTER_HELPER_PATH` | When `VF_MASTER_HOST` set | — | Absolute path to the PHP helper script on the master node |
| `VF_MASTER_PHP_BIN` | No | `/opt/virtfusion/php/bin/php` | Path to the PHP binary on the master node |
| `VF_MASTER_SSH_BIN` | No | `ssh` | SSH client binary |

## WHMCS Product Map

The product map file (`whmcsmap.conf` by default) maps old Virtualizor-era WHMCS product IDs to new VirtFusion product IDs. One mapping per line, `old_id=new_id`. Comments start with `#`.

```
# old WHMCS product ID = new WHMCS product ID
100=200
101=201
```

Copy `whmcsmap.conf.example` to `whmcsmap.conf` and edit to match your environment. This file is only required when `--whmcs` is used.

## Usage

```
vz2vf-migrate [-h] [--execute] [--single-vps VPSID[,VPSID,...]]
              [--continue-on-error] [--skip-inject] [--skip-shutdown]
              [--whmcs] [--env-file ENV_FILE]
              serid
```

**Dry-run an entire node:**

```bash
vz2vf-migrate 1
```

**Execute migration for a single VPS:**

```bash
vz2vf-migrate 1 --execute --single-vps 42
```

**Execute multiple specific VMs:**

```bash
vz2vf-migrate 1 --execute --single-vps 42,43,44
```

**Execute with WHMCS billing rewrite:**

```bash
vz2vf-migrate 1 --execute --whmcs --single-vps 42
```

**Continue after failures:**

```bash
vz2vf-migrate 1 --execute --continue-on-error
```

**Skip guest agent injection:**

```bash
vz2vf-migrate 1 --execute --skip-inject --single-vps 42
```

**Skip source shutdown (for pre-stopped VMs):**

```bash
vz2vf-migrate 1 --execute --skip-shutdown --single-vps 42
```

**Use a custom env file:**

```bash
vz2vf-migrate 1 --execute --env-file /etc/vz2vf/production.env
```

## Migration Pipeline

Each VM passes through the following stages in order. Completed stages are recorded in `migration_state.json` and skipped on re-run.

1. **resolve_whmcs** — look up the WHMCS service record by VPSID (database A, then fallback to B)
2. **resolve_user** — find or create a matching VirtFusion user via the API using the WHMCS client's identity
3. **create_server** — provision a new VirtFusion server with matching CPU, RAM, disk, and bandwidth specs
4. **build_server** — wait for VirtFusion to finish commissioning the server (polling until ready)
5. **preserve_hostname** — set the VirtFusion server name to match the source VM's hostname
6. **shutdown_source** — graceful `virsh shutdown` of the source VM, with polling until `shut off`
7. **copy_disks** — `rbd rm` + `rbd cp` for each disk (primary first, then additional disks)
8. **inject_guest** — `virt-customize` to install qemu-guest-agent and cloud-init inside the disk image
9. **add_ipv4** — reassign primary and additional IPv4 addresses via the VirtFusion API
10. **preserve_ipv6** — assign IPv6 subnet and addresses via the PHP helper on the VirtFusion master node. Execute-mode IPv6 preservation auto-creates the /64 subnet row in VirtFusion if one does not already exist for the source VM's IPv6 block
11. **rewrite_whmcs** — update `tblhosting` and `mod_virtfusion_direct` in the appropriate WHMCS database
12. **boot_target** — power on the VirtFusion server via the API

Guest injection is automatically skipped for MikroTik/RouterOS/CHR/Windows VMs. IPv6 preservation is skipped when the source VM has no IPv6 addresses. WHMCS rewrite only runs when `--whmcs` is passed.

## Output Files

Each run produces a unique `run_id` timestamp (e.g., `20260420T120000Z`).

- **`migration-summary-{serid}-{run_id}.csv`** — one row per VM with columns: `vpsid`, `vps_name`, `hostname`, `old_rbd_image`, `new_vf_server_id`, `new_rbd_image`, `ip_addresses`, `status`, `notes`
- **`logs/{run_id}/run.log`** — all commands executed during the run (with passwords redacted)
- **`logs/{run_id}/vm-{vpsid}.json`** — per-VM artifact with full stage details, source VM data, VirtFusion responses, WHMCS match data, and timing information
