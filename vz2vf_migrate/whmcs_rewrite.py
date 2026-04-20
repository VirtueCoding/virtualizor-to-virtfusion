def build_assignedips(additional_ipv4: list[str], historical_ipv6: list[str]) -> str:
    return "\n".join([*additional_ipv4, *historical_ipv6])


def _sql_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def build_tblhosting_update_query(
    hosting_id: int,
    product_id: int,
    server_id: int,
    domain: str,
    dedicated_ip: str,
    assigned_ips: str,
) -> str:
    return (
        "UPDATE tblhosting SET "
        f"packageid = {product_id}, "
        f"server = {server_id}, "
        f"domain = '{_sql_quote(domain)}', "
        f"dedicatedip = '{_sql_quote(dedicated_ip)}', "
        f"assignedips = '{_sql_quote(assigned_ips)}' "
        f"WHERE id = {hosting_id};"
    )


def build_mod_virtfusion_direct_update_query(
    hosting_id: int, server_id: int, server_object_json: str
) -> str:
    escaped = _sql_quote(server_object_json)
    return (
        "UPDATE mod_virtfusion_direct SET "
        f"server_id = {server_id}, "
        f"server_object = '{escaped}' "
        f"WHERE service_id = {hosting_id};"
    )


def build_mod_virtfusion_direct_insert_query(
    hosting_id: int, server_id: int, server_object_json: str
) -> str:
    escaped = _sql_quote(server_object_json)
    return (
        "INSERT INTO mod_virtfusion_direct (service_id, server_id, server_object) "
        f"SELECT {hosting_id}, {server_id}, '{escaped}' "
        f"WHERE NOT EXISTS (SELECT 1 FROM mod_virtfusion_direct WHERE service_id = {hosting_id});"
    )


def build_tblhosting_verify_query(
    hosting_id: int,
    product_id: int,
    server_id: int,
    domain: str,
    dedicated_ip: str,
    assigned_ips: str,
) -> str:
    return (
        "SELECT COUNT(*) INTO @tblhosting_match FROM tblhosting "
        f"WHERE id = {hosting_id} "
        f"AND packageid = {product_id} "
        f"AND server = {server_id} "
        f"AND domain = '{_sql_quote(domain)}' "
        f"AND dedicatedip = '{_sql_quote(dedicated_ip)}' "
        f"AND assignedips = '{_sql_quote(assigned_ips)}';"
    )


def build_mod_virtfusion_direct_verify_query(hosting_id: int, server_id: int, server_object_json: str) -> str:
    escaped = _sql_quote(server_object_json)
    return (
        "SELECT COUNT(*) INTO @mod_direct_match FROM mod_virtfusion_direct "
        f"WHERE service_id = {hosting_id} "
        f"AND server_id = {server_id} "
        f"AND server_object = '{escaped}';"
    )


def build_mysql_transaction_script(*queries: str) -> str:
    normalized = [query.strip().rstrip(";") + ";" for query in queries]
    while len(normalized) < 5:
        normalized.append("SELECT 0;")
    return "\n".join(
        [
            "START TRANSACTION;",
            normalized[0],
            "SET @tblhosting_rows = ROW_COUNT();",
            normalized[1],
            "SET @mod_update_rows = ROW_COUNT();",
            normalized[2],
            "SET @mod_insert_rows = ROW_COUNT();",
            normalized[3],
            normalized[4],
            "SET @rewrite_ok = IF(@tblhosting_match = 1 AND @mod_direct_match = 1, 1, 0);",
            "SET @final_sql = IF(@rewrite_ok = 1, 'COMMIT', 'ROLLBACK');",
            "PREPARE stmt FROM @final_sql;",
            "EXECUTE stmt;",
            "DEALLOCATE PREPARE stmt;",
            "SELECT @tblhosting_rows, @mod_update_rows, @mod_insert_rows, @rewrite_ok;",
        ]
    )


def parse_mysql_transaction_result(stdout: str) -> dict[str, int]:
    line = stdout.strip().splitlines()[-1] if stdout.strip() else ""
    parts = line.split("\t")
    if len(parts) != 4:
        raise ValueError("Unexpected WHMCS transaction result output")
    keys = ("tblhosting_rows", "mod_update_rows", "mod_insert_rows", "rewrite_ok")
    try:
        values = [int(part) for part in parts]
    except ValueError as exc:
        raise ValueError("Unexpected WHMCS transaction result output") from exc
    return dict(zip(keys, values, strict=True))
