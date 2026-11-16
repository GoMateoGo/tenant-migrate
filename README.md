# tenant-migrate

一个用 Go 和 Goose 编写的 PostgreSQL 多租户 schema 迁移工具。租户各自拥有一个 `tenant_*` schema，`tenant_1` 是结构模板，每个租户独立记录迁移版本。

## 能做什么

- 从 `tenant_1` 导出基线，为已有租户登记版本，或在空 schema 中演练迁移。
- 比较租户结构、查看迁移版本并接管由应用创建的新租户。
- 先迁移模板，再迁移其他租户。重跑时跳过已经应用的版本。

要求 Go 1.26.0 或兼容版本。使用 `compare` 或 `dump-baseline` 时，需要能连接目标 PostgreSQL 的 `pg_dump`。

## 快速上手

以下示例使用 PowerShell 7。先准备一个可丢弃、没有业务数据的 PostgreSQL 数据库。输入该库的 DSN，再创建两个空 schema：

```powershell
$env:MIGRATE_DSN = Read-Host -MaskInput '输入独立演练库的 PostgreSQL DSN'
psql -X -w -v ON_ERROR_STOP=1 --dbname "$env:MIGRATE_DSN" -c 'CREATE SCHEMA tenant_1; CREATE SCHEMA tenant_2;'
if ($LASTEXITCODE -ne 0) { throw '演练 schema 创建失败' }
go run ./cmd/migrate -dir examples/migrations/tenant fresh
go run ./cmd/migrate -dir examples/migrations/tenant status
go run ./cmd/migrate -dir examples/migrations/tenant check
Remove-Item Env:MIGRATE_DSN
```

示例目录含有 3 个迁移文件：建示例表、加列、删列。一次 `fresh` 会执行全部版本，最终版本为 3，示例列不存在。已有业务表的租户不要用 `fresh`。

```powershell
go build -o migrate.exe ./cmd/migrate
.\migrate.exe -h
```

完整命令、Jenkins 发布流程和一次上线演练见 [数据迁移步骤](数据迁移步骤.md)。[示例迁移](examples/migrations/tenant/) 使用虚构的表结构，可在隔离数据库练习。
