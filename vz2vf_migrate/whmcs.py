import csv
import io

from vz2vf_migrate.config import DatabaseConfig
from vz2vf_migrate.models import WhmcsMatch
from vz2vf_migrate.runner import CommandRunner

MYSQL_QUERY_TIMEOUT_SECONDS = 30


def build_whmcs_lookup_query(vpsid: int) -> str:
    return f"""
SELECT DISTINCT
    'whmcs_lookup' AS source_name,
    c.id AS client_id,
    c.firstname,
    c.lastname,
    c.email,
    p.id AS product_id,
    p.name AS product_name,
    h.id AS hosting_id,
    h.domain
FROM tblcustomfieldsvalues AS cfv
JOIN tblcustomfields AS cf
    ON cf.id = cfv.fieldid
   AND cf.fieldname = 'vpsid'
JOIN tblhosting AS h
    ON h.id = cfv.relid
JOIN tblclients AS c
    ON c.id = h.userid
JOIN tblproducts AS p
    ON p.id = h.packageid
WHERE TRIM(cfv.value) = '{vpsid}'
ORDER BY h.id
""".strip()


def parse_tsv(text: str) -> list[dict[str, str]]:
    if not text.strip():
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return [dict(row) for row in reader]


def _validate_whmcs_rows(rows: list[dict[str, str]]) -> None:
    if rows and not {"client_id", "hosting_id"}.issubset(rows[0]):
        raise RuntimeError("Malformed WHMCS query output: missing required columns")


class WhmcsRepository:
    def __init__(
        self,
        runner: CommandRunner,
        mysql_bin: str,
        primary_db: DatabaseConfig,
        fallback_db: DatabaseConfig,
    ) -> None:
        self.runner = runner
        self.mysql_bin = mysql_bin
        self.primary_db = primary_db
        self.fallback_db = fallback_db

    def _mysql_command(self, db: DatabaseConfig, query: str) -> list[str]:
        return [
            self.mysql_bin,
            "--batch",
            "--raw",
            "-u",
            db.user,
            "-h",
            db.host,
            "-P",
            str(db.port),
            db.name,
            "-e",
            query,
        ]

    def _query(self, db: DatabaseConfig, source_name: str, vpsid: int) -> list[dict[str, str]]:
        result = self.runner.run_readonly(
            self._mysql_command(db, build_whmcs_lookup_query(vpsid)),
            timeout=MYSQL_QUERY_TIMEOUT_SECONDS,
            env={"MYSQL_PWD": db.password},
        )
        if result.returncode != 0:
            detail = result.stderr.strip() or f"mysql exited with code {result.returncode}"
            raise RuntimeError(f"WHMCS query failed for {source_name}: {detail}")
        rows = parse_tsv(result.stdout)
        _validate_whmcs_rows(rows)
        for row in rows:
            row["source_name"] = source_name
        return rows

    @staticmethod
    def _raise_on_ambiguous(source_name: str, vpsid: int, rows: list[dict[str, str]]) -> None:
        if len(rows) <= 1:
            return
        hosting_ids = ", ".join(sorted({row.get("hosting_id", "?") for row in rows}))
        raise RuntimeError(
            f"Ambiguous WHMCS service match for VPS {vpsid} in {source_name}: hosting IDs {hosting_ids}"
        )

    def find_service(self, vpsid: int) -> WhmcsMatch | None:
        primary_rows = self._query(self.primary_db, "whmcs_a", vpsid)
        self._raise_on_ambiguous("whmcs_a", vpsid, primary_rows)
        row = primary_rows[0] if primary_rows else None
        if row is None:
            fallback_rows = self._query(self.fallback_db, "whmcs_b", vpsid)
            self._raise_on_ambiguous("whmcs_b", vpsid, fallback_rows)
            row = fallback_rows[0] if fallback_rows else None
        if row is None:
            return None
        return WhmcsMatch(
            source_name=row["source_name"],
            client_id=int(row["client_id"]),
            firstname=row["firstname"],
            lastname=row["lastname"],
            email=row["email"],
            product_id=int(row["product_id"]),
            product_name=row["product_name"],
            hosting_id=int(row["hosting_id"]),
            domain=row["domain"],
        )
