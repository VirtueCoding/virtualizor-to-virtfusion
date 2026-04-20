#!/usr/bin/env php
<?php

declare(strict_types=1);

const DEFAULT_APP_ROOT = '/opt/virtfusion';

main($argv);

function main(array $argv): void
{
    try {
        $options = parseOptions($argv);
        $payload = validatePayload(readPayload());

        if ($options['planOnly']) {
            writeJson(successResponse(buildResponseData($payload, null, null, false)));
            exit(0);
        }

        bootstrapVirtFusion($options['appRoot']);

        $data = \Illuminate\Support\Facades\DB::transaction(
            static fn () => claimIpv6Assignment($payload)
        );

        writeJson(successResponse($data));
        exit(0);
    } catch (\Throwable $exception) {
        writeJson(errorResponse($exception->getMessage()));
        exit(1);
    }
}

function parseOptions(array $argv): array
{
    $options = [
        'planOnly' => false,
        'appRoot' => getenv('VF_MASTER_APP_ROOT') ?: DEFAULT_APP_ROOT,
    ];

    foreach (array_slice($argv, 1) as $argument) {
        if ($argument === '--plan-only') {
            $options['planOnly'] = true;
            continue;
        }

        if (str_starts_with($argument, '--app-root=')) {
            $options['appRoot'] = substr($argument, strlen('--app-root='));
            continue;
        }

        throw new \InvalidArgumentException(sprintf('Unknown option: %s', $argument));
    }

    if (!is_string($options['appRoot']) || trim($options['appRoot']) === '') {
        throw new \InvalidArgumentException('app root must be a non-empty string');
    }

    $options['appRoot'] = rtrim($options['appRoot'], '/');

    return $options;
}

function readPayload(): array
{
    $raw = stream_get_contents(STDIN);
    if ($raw === false || trim($raw) === '') {
        throw new \InvalidArgumentException('STDIN must contain a JSON payload');
    }

    try {
        $decoded = json_decode($raw, true, 512, JSON_THROW_ON_ERROR);
    } catch (\JsonException $exception) {
        throw new \InvalidArgumentException('Invalid JSON input', 0, $exception);
    }

    if (!is_array($decoded)) {
        throw new \InvalidArgumentException('Payload must be a JSON object');
    }

    return $decoded;
}

function validatePayload(array $payload): array
{
    foreach (['action', 'serverId', 'subnet', 'cidr', 'addresses'] as $requiredKey) {
        if (!array_key_exists($requiredKey, $payload)) {
            throw new \InvalidArgumentException(sprintf('Missing required key: %s', $requiredKey));
        }
    }

    if ($payload['action'] !== 'preserve_ipv6') {
        throw new \InvalidArgumentException('Invalid action: expected "preserve_ipv6"');
    }

    $serverId = normalizePositiveInt($payload['serverId'], 'serverId');
    $subnet = normalizeIpv6String($payload['subnet'], 'subnet');
    $cidr = normalizePositiveInt($payload['cidr'], 'cidr');
    if ($cidr < 1 || $cidr > 128) {
        throw new \InvalidArgumentException('cidr must be between 1 and 128');
    }

    if (!is_array($payload['addresses']) || count($payload['addresses']) === 0) {
        throw new \InvalidArgumentException('addresses must be a non-empty array');
    }

    $addresses = [];
    foreach ($payload['addresses'] as $address) {
        $normalized = normalizeIpv6String($address, 'address');
        if (!ipv6MatchesSubnet($normalized, $subnet, $cidr)) {
            throw new \InvalidArgumentException(
                sprintf('address %s does not belong to %s/%d', $normalized, $subnet, $cidr)
            );
        }
        if (!in_array($normalized, $addresses, true)) {
            $addresses[] = $normalized;
        }
    }

    if ($addresses === []) {
        throw new \InvalidArgumentException('addresses must be a non-empty array');
    }

    return [
        'action' => 'preserve_ipv6',
        'serverId' => $serverId,
        'subnet' => $subnet,
        'cidr' => $cidr,
        'addresses' => $addresses,
    ];
}

function normalizePositiveInt(mixed $value, string $field): int
{
    if (is_int($value)) {
        $normalized = $value;
    } elseif (is_string($value) && preg_match('/^\d+$/', $value) === 1) {
        $normalized = (int) $value;
    } else {
        throw new \InvalidArgumentException(sprintf('%s must be a positive integer', $field));
    }

    if ($normalized < 1) {
        throw new \InvalidArgumentException(sprintf('%s must be a positive integer', $field));
    }

    return $normalized;
}

function normalizeIpv6String(mixed $value, string $field): string
{
    if (!is_string($value)) {
        throw new \InvalidArgumentException(sprintf('%s must be a non-empty string', $field));
    }

    $normalized = trim($value);
    if ($normalized === '') {
        throw new \InvalidArgumentException(sprintf('%s must be a non-empty string', $field));
    }

    if (filter_var($normalized, FILTER_VALIDATE_IP, FILTER_FLAG_IPV6) === false) {
        throw new \InvalidArgumentException(sprintf('%s must be a valid IPv6 address', $field));
    }

    $binary = inet_pton($normalized);
    $canonical = $binary === false ? false : inet_ntop($binary);
    if ($canonical === false) {
        throw new \InvalidArgumentException(sprintf('%s must be a valid IPv6 address', $field));
    }

    return strtolower($canonical);
}

function ipv6MatchesSubnet(string $address, string $subnet, int $cidr): bool
{
    $addressBinary = inet_pton($address);
    $subnetBinary = inet_pton($subnet);
    if ($addressBinary === false || $subnetBinary === false) {
        return false;
    }

    $fullBytes = intdiv($cidr, 8);
    $remainingBits = $cidr % 8;

    if ($fullBytes > 0 && substr($addressBinary, 0, $fullBytes) !== substr($subnetBinary, 0, $fullBytes)) {
        return false;
    }

    if ($remainingBits === 0) {
        return true;
    }

    $mask = 0xFF << (8 - $remainingBits);
    $addressByte = ord($addressBinary[$fullBytes]);
    $subnetByte = ord($subnetBinary[$fullBytes]);

    return ($addressByte & $mask) === ($subnetByte & $mask);
}

function successResponse(array $data): array
{
    return [
        'success' => true,
        'data' => $data,
    ];
}

function errorResponse(string $message): array
{
    return [
        'success' => false,
        'error' => [
            'message' => $message,
        ],
    ];
}

function buildResponseData(array $payload, ?int $interfaceId, ?int $subnetId, bool $subnetCreated = false): array
{
    return [
        'serverId' => $payload['serverId'],
        'interfaceId' => $interfaceId,
        'subnetId' => $subnetId,
        'subnet' => $payload['subnet'],
        'cidr' => $payload['cidr'],
        'addresses' => $payload['addresses'],
        'subnetCreated' => $subnetCreated,
    ];
}

function writeJson(array $payload): void
{
    echo json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES) . PHP_EOL;
}

function bootstrapVirtFusion(string $appRoot): void
{
    $candidateRoots = [$appRoot . '/app/control', $appRoot];
    foreach ($candidateRoots as $candidateRoot) {
        $autoloadPath = $candidateRoot . '/vendor/autoload.php';
        $bootstrapPath = $candidateRoot . '/bootstrap/app.php';

        if (!is_file($autoloadPath) || !is_file($bootstrapPath)) {
            continue;
        }

        require_once $autoloadPath;
        $app = require $bootstrapPath;

        if (!is_object($app) || !method_exists($app, 'make')) {
            throw new \RuntimeException('VirtFusion bootstrap did not return a Laravel application');
        }

        $kernel = $app->make(\Illuminate\Contracts\Console\Kernel::class);
        $kernel->bootstrap();
        return;
    }

    $autoloadPaths = array_map(
        static fn (string $root): string => $root . '/vendor/autoload.php',
        $candidateRoots
    );
    $bootstrapPaths = array_map(
        static fn (string $root): string => $root . '/bootstrap/app.php',
        $candidateRoots
    );

    if (array_filter($autoloadPaths, 'is_file') === []) {
        throw new \RuntimeException(sprintf('VirtFusion autoload not found: %s', implode(', ', $autoloadPaths)));
    }

    if (array_filter($bootstrapPaths, 'is_file') === []) {
        throw new \RuntimeException(sprintf('VirtFusion bootstrap not found: %s', implode(', ', $bootstrapPaths)));
    }

    throw new \RuntimeException(
        sprintf(
            'VirtFusion application root is inconsistent; expected matching autoload/bootstrap under one of: %s',
            implode(', ', $candidateRoots)
        )
    );
}

function claimIpv6Assignment(array $payload): array
{
    $serverTable = resolveTable(['servers']);
    $server = \Illuminate\Support\Facades\DB::table($serverTable)
        ->where('id', $payload['serverId'])
        ->first();

    if ($server === null) {
        throw new \RuntimeException(sprintf('Server %d was not found', $payload['serverId']));
    }

    $interfaceTable = resolveTable(['server_network_interfaces']);
    $interfaceMeta = describeColumns($interfaceTable);
    $interface = loadPrimaryInterface($interfaceTable, $interfaceMeta, $payload['serverId']);

    $subnetTable = resolveTable(['ipv6_subnets']);
    $subnetMeta = describeColumns($subnetTable);

    $blockTable = resolveTable(['ip_blocks']);
    $blockMeta = describeColumns($blockTable);

    $subnetRecord = claimSubnetRow(
        $subnetTable,
        $subnetMeta,
        $blockTable,
        $blockMeta,
        $interfaceTable,
        $interfaceMeta,
        $serverTable,
        $server,
        $interface,
        $payload
    );

    $addressTable = resolveTable(['ipv6']);
    $addressMeta = describeColumns($addressTable);
    upsertAddressRows(
        $addressTable,
        $addressMeta,
        $subnetTable,
        $subnetMeta,
        $interfaceTable,
        $payload['serverId'],
        $subnetRecord,
        $payload
    );

    return buildResponseData(
        $payload,
        (int) $interface->id,
        (int) $subnetRecord['id'],
        $subnetRecord['subnetCreated']
    );
}

function resolveTable(array $candidates): string
{
    foreach ($candidates as $table) {
        if (\Illuminate\Support\Facades\Schema::hasTable($table)) {
            return $table;
        }
    }

    throw new \RuntimeException(sprintf('Unable to resolve VirtFusion table from: %s', implode(', ', $candidates)));
}

function describeColumns(string $table): array
{
    $columns = [];
    foreach (\Illuminate\Support\Facades\Schema::getColumnListing($table) as $column) {
        $columns[$column] = true;
    }

    return $columns;
}

function resolveColumn(array $columns, array $candidates, string $label, bool $required = true): ?string
{
    foreach ($candidates as $candidate) {
        if (isset($columns[$candidate])) {
            return $candidate;
        }
    }

    if (!$required) {
        return null;
    }

    throw new \RuntimeException(sprintf('Unable to resolve required column %s', $label));
}

function loadPrimaryInterface(string $table, array $columns, int $serverId): object
{
    $serverIdColumn = resolveColumn($columns, ['server_id'], 'server id');
    $enabledColumn = resolveColumn($columns, ['enabled', 'is_enabled'], 'enabled', false);
    $primaryColumn = resolveColumn($columns, ['primary', 'is_primary'], 'primary', false);
    $defaultColumn = resolveColumn($columns, ['default', 'is_default'], 'default', false);
    $orderColumn = resolveColumn($columns, ['order', 'position'], 'order', false);

    $query = \Illuminate\Support\Facades\DB::table($table)->where($serverIdColumn, $serverId);
    if ($enabledColumn !== null) {
        $query->where($enabledColumn, 1);
    }
    if ($primaryColumn !== null) {
        $query->orderByDesc($primaryColumn);
    }
    if ($defaultColumn !== null) {
        $query->orderByDesc($defaultColumn);
    }
    if ($orderColumn !== null) {
        $query->orderBy($orderColumn);
    }

    $interface = $query->orderBy('id')->first();
    if ($interface === null) {
        throw new \RuntimeException(sprintf('Server %d has no primary network interface', $serverId));
    }

    return $interface;
}

function resolveOptionalTable(array $candidates): ?string
{
    foreach ($candidates as $table) {
        if (\Illuminate\Support\Facades\Schema::hasTable($table)) {
            return $table;
        }
    }

    return null;
}

function claimSubnetRow(
    string $table,
    array $columns,
    string $blockTable,
    array $blockColumns,
    string $interfaceTable,
    array $interfaceColumns,
    string $serverTable,
    object $server,
    object $interface,
    array $payload
): array
{
    $interfaceIdColumn = resolveColumn(
        $columns,
        ['server_network_interface_id', 'interface_id'],
        'subnet interface id'
    );
    $addressColumn = resolveColumn($columns, ['address', 'subnet'], 'subnet address');
    $cidrColumn = resolveColumn($columns, ['cidr'], 'cidr');
    $blockIdColumn = resolveColumn($columns, ['ip_block_id', 'block_id'], 'block id');
    $serverIdColumn = resolveColumn($columns, ['server_id'], 'server id', false);
    $enabledColumn = resolveColumn($columns, ['enabled', 'is_enabled'], 'enabled', false);
    $interfaceSecondaryColumn = resolveColumn($columns, ['interface_secondary'], 'interface secondary', false);
    $reservedColumn = resolveColumn($columns, ['reserved'], 'reserved', false);
    $orderColumn = resolveColumn($columns, ['order', 'position'], 'order', false);
    $exhaustedColumn = resolveColumn($columns, ['exhausted'], 'exhausted', false);
    $createdAtColumn = resolveColumn($columns, ['created_at'], 'created at', false);
    $updatedAtColumn = resolveColumn($columns, ['updated_at'], 'updated at', false);
    $requestedSubnet = \Illuminate\Support\Facades\DB::table($table)
        ->where($addressColumn, $payload['subnet'])
        ->where($cidrColumn, $payload['cidr'])
        ->first();

    $subnetCreated = false;

    if ($requestedSubnet === null) {
        $candidateBlocks = findParentBlocks(
            $blockTable,
            $blockColumns,
            $payload['subnet'],
            $payload['cidr']
        );

        if (empty($candidateBlocks)) {
            throw new \RuntimeException(
                sprintf(
                    'No parent IPv6 block found for subnet %s/%d',
                    $payload['subnet'],
                    $payload['cidr']
                )
            );
        }

        $parentBlock = null;
        $lastMappingError = null;
        foreach ($candidateBlocks as $candidate) {
            try {
                assertBlockMapsToTargetNetwork(
                    $blockTable,
                    $blockColumns,
                    $interfaceColumns,
                    $serverTable,
                    $server,
                    $interface,
                    $candidate
                );
                $parentBlock = $candidate;
                break;
            } catch (\RuntimeException $e) {
                $lastMappingError = $e;
            }
        }

        if ($parentBlock === null) {
            throw $lastMappingError ?? new \RuntimeException(
                'No parent block maps to target hypervisor/network'
            );
        }

        $timestamp = date('Y-m-d H:i:s');
        $insertAttributes = [
            $addressColumn => $payload['subnet'],
            $cidrColumn => $payload['cidr'],
            $blockIdColumn => (int) $parentBlock->id,
        ];
        if ($interfaceSecondaryColumn !== null) {
            $insertAttributes[$interfaceSecondaryColumn] = 0;
        }
        if ($reservedColumn !== null) {
            $insertAttributes[$reservedColumn] = 0;
        }
        if ($orderColumn !== null) {
            $insertAttributes[$orderColumn] = 1;
        }
        if ($exhaustedColumn !== null) {
            $insertAttributes[$exhaustedColumn] = 0;
        }
        if ($createdAtColumn !== null) {
            $insertAttributes[$createdAtColumn] = $timestamp;
        }
        if ($updatedAtColumn !== null) {
            $insertAttributes[$updatedAtColumn] = $timestamp;
        }

        $newId = \Illuminate\Support\Facades\DB::table($table)->insertGetId($insertAttributes);
        $requestedSubnet = \Illuminate\Support\Facades\DB::table($table)
            ->where('id', $newId)
            ->first();
        $subnetCreated = true;
    } else {
        assertSubnetMapsToTargetNetwork(
            $blockTable,
            $blockColumns,
            $blockIdColumn,
            $interfaceColumns,
            $serverTable,
            $server,
            $interface,
            $requestedSubnet
        );
    }

    $interfaceConflict = \Illuminate\Support\Facades\DB::table($table)
        ->where($interfaceIdColumn, $interface->id)
        ->where('id', '!=', $requestedSubnet->id)
        ->first();

    if ($interfaceConflict !== null) {
        throw new \RuntimeException(
            sprintf(
                'Interface %d already has a different IPv6 subnet row (%s/%d)',
                (int) $interface->id,
                (string) $interfaceConflict->{$addressColumn},
                (int) $interfaceConflict->{$cidrColumn}
            )
        );
    }

    $assignedInterfaceId = $requestedSubnet->{$interfaceIdColumn} ?? null;
    if ($assignedInterfaceId !== null && (int) $assignedInterfaceId !== (int) $interface->id) {
        $assignedInterface = \Illuminate\Support\Facades\DB::table($interfaceTable)
            ->where('id', (int) $assignedInterfaceId)
            ->first();
        $assignedServerId = $assignedInterface !== null && isset($assignedInterface->server_id)
            ? (int) $assignedInterface->server_id
            : null;
        throw new \RuntimeException(
            sprintf(
                'Requested subnet %s/%d is already assigned to another server%s',
                $payload['subnet'],
                $payload['cidr'],
                $assignedServerId === null ? '' : sprintf(' (%d)', $assignedServerId)
            )
        );
    }

    $attributes = [
        $interfaceIdColumn => (int) $interface->id,
    ];
    if ($serverIdColumn !== null) {
        $attributes[$serverIdColumn] = $payload['serverId'];
    }
    if ($enabledColumn !== null) {
        $attributes[$enabledColumn] = 1;
    }

    $timestamp = date('Y-m-d H:i:s');
    if ($updatedAtColumn !== null) {
        $attributes[$updatedAtColumn] = $timestamp;
    }
    if ($createdAtColumn !== null && !isset($requestedSubnet->{$createdAtColumn})) {
        $attributes[$createdAtColumn] = $timestamp;
    }

    \Illuminate\Support\Facades\DB::table($table)
        ->where('id', $requestedSubnet->id)
        ->update($attributes);

    return ['id' => (int) $requestedSubnet->id, 'subnetCreated' => $subnetCreated];
}

function verifyBlockNetworkMapping(
    array $blockColumns,
    object $block,
    int $hypervisorNetworkId,
    object $server
): void {
    $mappingVerified = false;
    $mapped = false;

    if (\Illuminate\Support\Facades\Schema::hasTable('ip_block_hypervisor_network')) {
        $bhnColumns = describeColumns('ip_block_hypervisor_network');
        $bhnBlockCol = resolveColumn($bhnColumns, ['ip_block_id', 'block_id'], 'block id in ip_block_hypervisor_network');
        $bhnNetCol = resolveColumn($bhnColumns, ['hypervisor_network_id', 'network_id'], 'network id in ip_block_hypervisor_network');
        $mappingVerified = true;
        $mapped = \Illuminate\Support\Facades\DB::table('ip_block_hypervisor_network')
            ->where($bhnBlockCol, (int) $block->id)
            ->where($bhnNetCol, (int) $hypervisorNetworkId)
            ->exists();
    }

    if (!$mapped
        && \Illuminate\Support\Facades\Schema::hasTable('ip_block_group')
        && \Illuminate\Support\Facades\Schema::hasTable('ip_block_grp_hv_net')
    ) {
        $ghnColumns = describeColumns('ip_block_grp_hv_net');
        $ghnGroupCol = resolveColumn($ghnColumns, ['ip_block_group_id', 'group_id'], 'group id in ip_block_grp_hv_net');
        $ghnNetCol = resolveColumn($ghnColumns, ['hypervisor_network_id', 'network_id'], 'network id in ip_block_grp_hv_net');

        $groupIdColumn = resolveColumn($blockColumns, ['ip_block_group_id'], 'ip block group id', false);
        if ($groupIdColumn !== null && isset($block->{$groupIdColumn}) && $block->{$groupIdColumn} !== null) {
            // Entity-style: ip_block_group has an 'id' PK, blocks carry a FK
            $mappingVerified = true;
            $mapped = \Illuminate\Support\Facades\DB::table('ip_block_group')
                ->join(
                    'ip_block_grp_hv_net',
                    'ip_block_group.id',
                    '=',
                    'ip_block_grp_hv_net.' . $ghnGroupCol
                )
                ->where('ip_block_group.id', (int) $block->{$groupIdColumn})
                ->where('ip_block_grp_hv_net.' . $ghnNetCol, (int) $hypervisorNetworkId)
                ->exists();
        } else {
            // Pivot-style: ip_block_group is (group_id, block_id) with no 'id' PK
            $bgColumns = describeColumns('ip_block_group');
            $bgBlockCol = resolveColumn($bgColumns, ['block_id', 'ip_block_id'], 'block id in ip_block_group', false);
            $bgGroupCol = resolveColumn($bgColumns, ['group_id', 'ip_block_group_id'], 'group id in ip_block_group', false);
            if ($bgBlockCol !== null && $bgGroupCol !== null) {
                $mappingVerified = true;
                $mapped = \Illuminate\Support\Facades\DB::table('ip_block_group')
                    ->join(
                        'ip_block_grp_hv_net',
                        'ip_block_group.' . $bgGroupCol,
                        '=',
                        'ip_block_grp_hv_net.' . $ghnGroupCol
                    )
                    ->where('ip_block_group.' . $bgBlockCol, (int) $block->id)
                    ->where('ip_block_grp_hv_net.' . $ghnNetCol, (int) $hypervisorNetworkId)
                    ->exists();
            }
        }
    }

    if (!$mappingVerified) {
        $serverId = isset($server->id) ? (int) $server->id : 0;
        throw new \RuntimeException(sprintf('Unable to verify IP block mapping for server %d', $serverId));
    }

    if (!$mapped) {
        throw new \RuntimeException('Requested subnet does not map to target hypervisor/network');
    }
}

function assertSubnetMapsToTargetNetwork(
    string $blockTable,
    array $blockColumns,
    string $blockIdColumn,
    array $interfaceColumns,
    string $serverTable,
    object $server,
    object $interface,
    object $requestedSubnet
): void {
    $hypervisorNetworkColumn = resolveColumn(
        $interfaceColumns,
        ['hypervisor_network_id', 'hypervisor_network'],
        'interface hypervisor network'
    );
    $hypervisorNetworkId = $interface->{$hypervisorNetworkColumn} ?? null;
    if ($hypervisorNetworkId === null || $hypervisorNetworkId === '') {
        throw new \RuntimeException('Target interface has no hypervisor network mapping');
    }

    $blockId = $requestedSubnet->{$blockIdColumn} ?? null;
    if ($blockId === null || $blockId === '') {
        throw new \RuntimeException(sprintf('Requested subnet row %d is missing %s', (int) $requestedSubnet->id, $blockIdColumn));
    }

    $block = \Illuminate\Support\Facades\DB::table($blockTable)
        ->where('id', (int) $blockId)
        ->first();
    if ($block === null) {
        throw new \RuntimeException(sprintf('IP block %d referenced by subnet %d was not found', (int) $blockId, (int) $requestedSubnet->id));
    }

    verifyBlockNetworkMapping($blockColumns, $block, (int) $hypervisorNetworkId, $server);
}

function findParentBlocks(
    string $blockTable,
    array $blockColumns,
    string $subnet,
    int $cidr
): array {
    $ipv6SubnetColumn = resolveColumn($blockColumns, ['ipv6_subnet'], 'ipv6 subnet');
    $cidrFromColumn = resolveColumn($blockColumns, ['ipv6_cidr_from'], 'cidr from');
    $cidrToColumn = resolveColumn($blockColumns, ['ipv6_cidr_to'], 'cidr to');

    $blocks = \Illuminate\Support\Facades\DB::table($blockTable)
        ->whereNotNull($ipv6SubnetColumn)
        ->get();

    $candidates = [];

    foreach ($blocks as $block) {
        $blockSubnet = $block->{$ipv6SubnetColumn};
        if ($blockSubnet === null || $blockSubnet === '') {
            continue;
        }

        $normalizedBlockSubnet = normalizeIpv6String((string) $blockSubnet, 'block subnet');
        $blockCidrFrom = (int) $block->{$cidrFromColumn};
        $blockCidrTo = (int) $block->{$cidrToColumn};

        if ($cidr >= $blockCidrFrom
            && $cidr <= $blockCidrTo
            && ipv6MatchesSubnet($subnet, $normalizedBlockSubnet, $blockCidrFrom)
        ) {
            $candidates[] = ['block' => $block, 'prefix' => $blockCidrFrom];
        }
    }

    // Most specific (largest prefix) first
    usort($candidates, fn($a, $b) => $b['prefix'] <=> $a['prefix']);

    return array_map(fn($c) => $c['block'], $candidates);
}

function assertBlockMapsToTargetNetwork(
    string $blockTable,
    array $blockColumns,
    array $interfaceColumns,
    string $serverTable,
    object $server,
    object $interface,
    object $block
): void {
    $hypervisorNetworkColumn = resolveColumn(
        $interfaceColumns,
        ['hypervisor_network_id', 'hypervisor_network'],
        'interface hypervisor network'
    );
    $hypervisorNetworkId = $interface->{$hypervisorNetworkColumn} ?? null;
    if ($hypervisorNetworkId === null || $hypervisorNetworkId === '') {
        throw new \RuntimeException('Target interface has no hypervisor network mapping');
    }

    verifyBlockNetworkMapping($blockColumns, $block, (int) $hypervisorNetworkId, $server);
}

function upsertAddressRows(
    string $table,
    array $columns,
    string $subnetTable,
    array $subnetColumns,
    string $interfaceTable,
    int $serverId,
    array $subnetRecord,
    array $payload
): void
{
    $subnetInterfaceIdColumn = resolveColumn(
        $subnetColumns,
        ['server_network_interface_id', 'interface_id'],
        'subnet interface id'
    );
    $subnetIdColumn = resolveColumn(
        $columns,
        ['ipv6_subnet_id', 'subnet_id'],
        'address subnet id'
    );
    $addressColumn = resolveColumn($columns, ['address'], 'address');
    $enabledColumn = resolveColumn($columns, ['enabled', 'is_enabled'], 'enabled', false);
    $rdnsColumn = resolveColumn($columns, ['rdns'], 'rdns', false);
    $orderColumn = resolveColumn($columns, ['order', 'position'], 'order', false);
    $createdAtColumn = resolveColumn($columns, ['created_at'], 'created at', false);
    $updatedAtColumn = resolveColumn($columns, ['updated_at'], 'updated at', false);
    $timestamp = date('Y-m-d H:i:s');
    $subnetId = $subnetRecord['id'];
    $requestedAddresses = array_fill_keys($payload['addresses'], true);

    $existingRows = \Illuminate\Support\Facades\DB::table($table)
        ->where($subnetIdColumn, $subnetId)
        ->orderBy('id')
        ->get();

    $extraAddresses = [];
    foreach ($existingRows as $existingRow) {
        $existingAddress = normalizeIpv6String((string) $existingRow->{$addressColumn}, 'stored IPv6 address');
        if (!isset($requestedAddresses[$existingAddress])) {
            $extraAddresses[] = $existingAddress;
        }
    }

    if ($extraAddresses !== []) {
        throw new \RuntimeException(
            sprintf(
                'Claimed subnet %s/%d already has IPv6 addresses not present in the requested set: %s',
                $payload['subnet'],
                $payload['cidr'],
                implode(', ', $extraAddresses)
            )
        );
    }

    foreach ($payload['addresses'] as $index => $address) {
        $attributes = [
            $subnetIdColumn => $subnetId,
            $addressColumn => $address,
        ];
        if ($enabledColumn !== null) {
            $attributes[$enabledColumn] = 1;
        }
        if ($rdnsColumn !== null) {
            $attributes[$rdnsColumn] = null;
        }
        if ($orderColumn !== null) {
            $attributes[$orderColumn] = $index + 1;
        }
        if ($updatedAtColumn !== null) {
            $attributes[$updatedAtColumn] = $timestamp;
        }

        $conflict = \Illuminate\Support\Facades\DB::table($table)
            ->join($subnetTable, $subnetTable . '.id', '=', $table . '.' . $subnetIdColumn)
            ->join($interfaceTable, $interfaceTable . '.id', '=', $subnetTable . '.' . $subnetInterfaceIdColumn)
            ->where($table . '.' . $addressColumn, $address)
            ->where($table . '.' . $subnetIdColumn, '!=', $subnetId)
            ->select($interfaceTable . '.server_id as server_id')
            ->first();

        if ($conflict !== null) {
            $conflictServerId = (int) $conflict->server_id;
            if ($conflictServerId !== $serverId) {
                throw new \RuntimeException(
                    sprintf(
                        'Requested IPv6 address %s is already assigned to another server (%d)',
                        $address,
                        $conflictServerId
                    )
                );
            }

            throw new \RuntimeException(
                sprintf(
                    'Requested IPv6 address %s already exists outside claimed subnet %d',
                    $address,
                    $subnetId
                )
            );
        }

        $existing = \Illuminate\Support\Facades\DB::table($table)
            ->where($subnetIdColumn, $subnetId)
            ->where($addressColumn, $address)
            ->first();

        if ($existing !== null) {
            \Illuminate\Support\Facades\DB::table($table)
                ->where('id', $existing->id)
                ->update($attributes);
            continue;
        }

        if ($createdAtColumn !== null) {
            $attributes[$createdAtColumn] = $timestamp;
        }
        \Illuminate\Support\Facades\DB::table($table)->insert($attributes);
    }
}
