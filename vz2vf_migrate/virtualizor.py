import csv
import io

from vz2vf_migrate.config import DatabaseConfig
from vz2vf_migrate.runner import CommandRunner

MYSQL_QUERY_TIMEOUT_SECONDS = 30


def build_virtualizor_inventory_query(serid: int, single_vps: list[int] | None) -> str:
    where_parts = [f"v.serid = {serid}"]
    if single_vps:
        ids = ",".join(str(vpsid) for vpsid in single_vps)
        where_parts.append(f"v.vpsid IN ({ids})")
    where_clause = " AND ".join(where_parts)
    return f"""
SELECT
    v.vpsid,
    v.vps_name,
    v.uuid,
    v.uid,
    v.hostname,
    v.os_name,
    v.space,
    v.ram,
    v.cores,
    v.bandwidth,
    v.network_speed,
    v.upload_speed,
    d.did,
    d.path AS disk_path,
    d.primary AS disk_primary,
    d.size AS disk_size,
    d.size_unit AS disk_size_unit,
    i.ipid,
    i.ip,
    i.ipv6,
    i.ipr_netmask,
    i.primary AS ip_primary,
    u.email AS virtualizor_email
FROM vps AS v
LEFT JOIN disks AS d ON d.vps_uuid = v.uuid
LEFT JOIN ips AS i ON i.vpsid = v.vpsid
LEFT JOIN users AS u ON u.uid = v.uid
WHERE {where_clause}
ORDER BY v.vpsid, d.primary DESC, d.did, i.ipid
""".strip()


def parse_tsv(text: str) -> list[dict[str, str]]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [dict(row) for row in reader]


def _validate_inventory_rows(rows: list[dict[str, str]]) -> None:
    if rows and "vpsid" not in rows[0]:
        raise RuntimeError("Malformed Virtualizor query output: missing vpsid column")


class VirtualizorRepository:
    def __init__(self, runner: CommandRunner, mysql_bin: str, db: DatabaseConfig) -> None:
        self.runner = runner
        self.mysql_bin = mysql_bin
        self.db = db

    def build_mysql_command(self, query: str) -> list[str]:
        return [
            self.mysql_bin,
            "--batch",
            "--raw",
            "-u",
            self.db.user,
            "-h",
            self.db.host,
            "-P",
            str(self.db.port),
            self.db.name,
            "-e",
            query,
        ]

    def fetch_inventory_rows(self, serid: int, single_vps: list[int] | None) -> list[dict[str, str]]:
        result = self.runner.run_readonly(
            self.build_mysql_command(build_virtualizor_inventory_query(serid, single_vps)),
            timeout=MYSQL_QUERY_TIMEOUT_SECONDS,
            env={"MYSQL_PWD": self.db.password},
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or f"mysql exited with code {result.returncode}"
            raise RuntimeError(f"Virtualizor query failed: {detail}")
        rows = parse_tsv(result.stdout)
        _validate_inventory_rows(rows)
        return rows
