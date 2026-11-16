// Command migrate 负责所有 tenant_* schema 的表结构迁移（基于 goose）。
//
// 用法（连接串用 -dsn 传入，或设置环境变量 MIGRATE_DSN；使用有 DDL 权限的迁移专用账号）：
//
//	migrate -out <文件> dump-baseline           从 tenant_1 生成 00001_baseline.sql（需要 pg_dump）
//	migrate [flags] compare <期望schema> <实际schema>   完整结构比对（需要 pg_dump）
//	migrate [flags] baseline   一次性：把现有租户标记为已执行 00001_baseline（不实际执行）
//	migrate [flags] repair <租户schema>  根据 00001_baseline.sql 输出修复 SQL
//	migrate [flags] status     列出每个租户的当前版本和待执行数量
//	migrate [flags] adopt      接管没有版本记录、且结构与 tenant_1 一致的租户（定时任务用）
//	migrate [flags] up         接管 → 迁移 tenant_1 → 迁移其他租户
//	migrate [flags] check      所有租户与 tenant_1 做结构比对（字段 + 索引）
//	migrate [flags] fresh      只用于 CI 空库：对空 schema 从零执行全部迁移
//
// 迁移文件在运行时从 -dir 目录读取，不打包进二进制。
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

const (
	baseSchema    = "tenant_1"         // 标准租户：新租户由别人的脚本从它复制
	versionTable  = "goose_db_version" // goose 默认的版本表名
	globalLockKey = "tenant-migrate"   // 整个迁移过程的全局 advisory lock
)

var (
	flagDSN          = flag.String("dsn", "", "数据库连接串，不传则读环境变量 MIGRATE_DSN")
	flagDir          = flag.String("dir", "migrations/tenant", "迁移文件目录")
	flagConcurrency  = flag.Int("concurrency", 4, "同时迁移的租户数")
	flagAllowMissing = flag.Bool("allow-missing", false, "允许乱序执行迁移（只在测试环境开启）")
	flagLockTimeout  = flag.String("lock-timeout", "5s", "DDL 等待表锁的最长时间")

	schemaNameRe = regexp.MustCompile(`^tenant_[a-z0-9_]+$`)
)

type app struct {
	dsn      string
	db       *sql.DB // 不带 search_path 的公共连接，用于查询元数据
	dir      string
	fsys     fs.FS
	versions []int64 // 目录中所有迁移的版本号（升序）
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "用法: migrate [flags] <命令> [参数]")
		fmt.Fprintln(os.Stderr, "命令: status | check | compare <期望schema> <实际schema> | dump-baseline | baseline | repair <租户schema> | adopt | up | fresh")
		fmt.Fprintln(os.Stderr, "注意: flags 必须写在命令前面，例如 migrate -dir migrations/tenant up")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(2)
	}
	dsn := *flagDSN
	if dsn == "" {
		dsn = os.Getenv("MIGRATE_DSN")
	}
	if dsn == "" {
		fmt.Fprintln(os.Stderr, "缺少连接串：用 -dsn 传入，或设置环境变量 MIGRATE_DSN")
		os.Exit(2)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	a, err := newApp(ctx, dsn, *flagDir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "初始化失败:", err)
		os.Exit(1)
	}
	defer a.db.Close()

	switch flag.Arg(0) {
	case "dump-baseline":
		err = a.dumpBaseline(ctx)
	case "compare":
		err = a.compare(ctx, flag.Args()[1:])
	case "baseline":
		err = a.withGlobalLock(ctx, a.baseline)
	case "repair":
		err = a.repair(ctx, flag.Args()[1:])
		if err == nil {
			return
		}
	case "status":
		err = a.status(ctx)
	case "adopt":
		err = a.withGlobalLock(ctx, a.adoptAll)
	case "up":
		err = a.withGlobalLock(ctx, a.up)
	case "check":
		err = a.check(ctx)
	case "fresh":
		err = a.withGlobalLock(ctx, a.fresh)
	default:
		flag.Usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "\n失败:", err)
		os.Exit(1)
	}
	fmt.Println("\n完成")
}

func newApp(ctx context.Context, dsn, dir string) (*app, error) {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("解析 MIGRATE_DSN: %w", err)
	}
	db := stdlib.OpenDB(*cfg)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("连接数据库: %w", err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}
	var versions []int64
	for _, f := range files {
		v, err := goose.NumericComponent(filepath.Base(f))
		if err != nil {
			return nil, fmt.Errorf("迁移文件名不合法 %s: %w", f, err)
		}
		versions = append(versions, v)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	return &app{dsn: dsn, db: db, dir: dir, fsys: os.DirFS(dir), versions: versions}, nil
}

func (a *app) needMigrations() error {
	if len(a.versions) == 0 {
		return fmt.Errorf("目录 %s 中没有迁移文件（先用 dump-baseline 生成 00001_baseline.sql）", a.dir)
	}
	return nil
}

// ---------- 命令 ----------

// baseline：一次性操作。现有租户的结构已经存在，只写版本记录，不执行 00001_baseline.sql。
func (a *app) baseline(ctx context.Context) error {
	if err := a.needMigrations(); err != nil {
		return err
	}
	if a.versions[0] != 1 {
		return errors.New("第一个迁移文件必须是 00001_baseline.sql")
	}
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}

	// 先处理 tenant_1
	if ok, err := a.hasVersion(ctx, baseSchema); err != nil {
		return err
	} else if ok {
		fmt.Printf("[%s] 已有版本记录，跳过\n", baseSchema)
	} else {
		if err := a.markBase(ctx); err != nil {
			return fmt.Errorf("[%s] %w", baseSchema, err)
		}
		fmt.Printf("[%s] 已标记为版本 1\n", baseSchema)
	}

	// 再处理其他租户：结构必须和 tenant_1 一致才标记
	var bad []string
	for _, s := range schemas {
		if s == baseSchema {
			continue
		}
		if ok, err := a.hasVersion(ctx, s); err != nil {
			return err
		} else if ok {
			fmt.Printf("[%s] 已有版本记录，跳过\n", s)
			continue
		}
		diffs, err := a.columnDiff(ctx, s)
		if err != nil {
			return err
		}
		if len(diffs) > 0 {
			printDiffs(s, diffs)
			bad = append(bad, s)
			continue
		}
		if err := a.adopt(ctx, s, 1); err != nil {
			return fmt.Errorf("[%s] %w", s, err)
		}
		fmt.Printf("[%s] 已标记为版本 1\n", s)
	}
	if len(bad) > 0 {
		return fmt.Errorf("%d 个租户结构与 %s 不一致，未标记: %s（修平后重新执行 baseline）",
			len(bad), baseSchema, strings.Join(bad, ", "))
	}
	return nil
}

func (a *app) status(ctx context.Context) error {
	if err := a.needMigrations(); err != nil {
		return err
	}
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	latest := a.versions[len(a.versions)-1]
	fmt.Printf("迁移目录最新版本: %d\n\n%-24s %-10s %-8s %s\n", latest, "SCHEMA", "VERSION", "PENDING", "NOTE")

	behind := 0
	for _, s := range schemas {
		ok, err := a.hasVersion(ctx, s)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Printf("%-24s %-10s %-8s %s\n", s, "-", "-", "无版本记录（up 时会尝试接管）")
			behind++
			continue
		}
		v, err := a.currentVersion(ctx, s)
		if err != nil {
			return err
		}
		pending := 0
		for _, mv := range a.versions {
			if mv > v {
				pending++
			}
		}
		note := ""
		if pending > 0 {
			behind++
			note = "待迁移"
		}
		fmt.Printf("%-24s %-10d %-8d %s\n", s, v, pending, note)
	}
	fmt.Printf("\n共 %d 个租户，%d 个需要处理\n", len(schemas), behind)
	return nil
}

// adoptAll：定时任务使用。只接管结构与 tenant_1 一致的无版本租户，不执行迁移。
func (a *app) adoptAll(ctx context.Context) error {
	baseVer, err := a.currentVersion(ctx, baseSchema)
	if err != nil {
		return fmt.Errorf("[%s] 读取版本失败（是否已执行 baseline？）: %w", baseSchema, err)
	}
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	var bad []string
	for _, s := range schemas {
		if s == baseSchema {
			continue
		}
		adopted, err := a.tryAdopt(ctx, s, baseVer)
		if err != nil {
			bad = append(bad, s)
			fmt.Println(err)
			continue
		}
		if adopted {
			fmt.Printf("[%s] 已接管，版本 %d\n", s, baseVer)
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%d 个租户无法接管: %s", len(bad), strings.Join(bad, ", "))
	}
	return nil
}

func (a *app) up(ctx context.Context) error {
	if err := a.needMigrations(); err != nil {
		return err
	}
	// 第 0 步：记下 tenant_1 当前版本
	if ok, err := a.hasVersion(ctx, baseSchema); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("[%s] 没有版本记录，请先执行 baseline", baseSchema)
	}
	vOld, err := a.currentVersion(ctx, baseSchema)
	if err != nil {
		return err
	}
	fmt.Printf("[%s] 当前版本 %d\n", baseSchema, vOld)

	// 先完成只读预检，避免某个已有租户无法接管时，其他租户已经开始迁移。
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	var tenantsToMigrate, tenantsToAdopt []string
	var blockedTenants []error
	known := map[string]bool{}
	for _, s := range schemas {
		known[s] = true
		if s == baseSchema {
			continue
		}
		needsAdoption, err := a.checkAdoption(ctx, s)
		if err != nil {
			blockedTenants = append(blockedTenants, fmt.Errorf("[%s] %w", s, err))
			continue
		}
		if needsAdoption {
			tenantsToAdopt = append(tenantsToAdopt, s)
		}
		tenantsToMigrate = append(tenantsToMigrate, s)
	}
	if len(blockedTenants) > 0 {
		return fmt.Errorf("迁移预检失败，尚未写入版本记录或执行迁移: %w", errors.Join(blockedTenants...))
	}
	for _, s := range tenantsToAdopt {
		if err := a.adopt(ctx, s, vOld); err != nil {
			return fmt.Errorf("[%s] 写入接管版本失败，尚未执行业务迁移: %w", s, err)
		}
		fmt.Printf("[%s] 已接管，版本 %d\n", s, vOld)
	}

	// 第 2 步：先迁移 tenant_1，失败则整体中止
	if err := a.migrateSchema(ctx, baseSchema); err != nil {
		return fmt.Errorf("[%s] 迁移失败，其他租户未处理: %w", baseSchema, err)
	}
	failed := a.parallel(ctx, tenantsToMigrate, a.migrateSchema)

	// 第 3 步补充：迁移期间由脚本新建的租户
	latest, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	for _, s := range latest {
		if known[s] {
			continue
		}
		failed[s] = errors.New("迁移期间发现新租户，已跳过；请在重跑 up 或 adopt 前确认来源版本，已提交的迁移仍然生效")
	}

	if len(failed) > 0 {
		names := make([]string, 0, len(failed))
		for s, err := range failed {
			names = append(names, s)
			fmt.Fprintf(os.Stderr, "  %s: %v\n", s, err)
		}
		sort.Strings(names)
		return fmt.Errorf("%d 个租户失败: %s", len(failed), strings.Join(names, ", "))
	}
	return nil
}

// fresh：只用于 CI 空库。对没有任何业务表的租户从零执行全部迁移；发现有表就拒绝。
func (a *app) fresh(ctx context.Context) error {
	if err := a.needMigrations(); err != nil {
		return err
	}
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	for _, s := range schemas {
		var n int
		if err := a.db.QueryRowContext(ctx, `
			SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind IN ('r','p') AND c.relname <> 'goose_db_version'`, s).Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return fmt.Errorf("[%s] 不是空 schema（有 %d 张表），fresh 只能用于空库", s, n)
		}
	}
	// 先执行 tenant_1，再执行其他租户
	if err := a.migrateSchema(ctx, baseSchema); err != nil {
		return fmt.Errorf("[%s] %w", baseSchema, err)
	}
	for _, s := range schemas {
		if s == baseSchema {
			continue
		}
		if err := a.migrateSchema(ctx, s); err != nil {
			return fmt.Errorf("[%s] %w", s, err)
		}
	}
	return nil
}

func (a *app) check(ctx context.Context) error {
	schemas, err := a.tenants(ctx)
	if err != nil {
		return err
	}
	var bad []string
	for _, s := range schemas {
		if s == baseSchema {
			continue
		}
		cols, err := a.columnDiff(ctx, s)
		if err != nil {
			return err
		}
		idx, err := a.indexDiff(ctx, s)
		if err != nil {
			return err
		}
		ok, err := a.hasVersion(ctx, s)
		if err != nil {
			return err
		}
		if !ok {
			fmt.Printf("[%s] 没有版本记录\n", s)
		}
		if len(cols)+len(idx) > 0 || !ok {
			printDiffs(s, append(cols, idx...))
			bad = append(bad, s)
		}
	}
	if len(bad) > 0 {
		return fmt.Errorf("%d 个租户与 %s 不一致: %s", len(bad), baseSchema, strings.Join(bad, ", "))
	}
	fmt.Printf("%d 个租户与 %s 结构一致\n", len(schemas)-1, baseSchema)
	return nil
}

// ---------- 接管 ----------

// tryAdopt：已有版本记录 → 返回 false；无记录且结构一致 → 接管为 ver；结构不一致 → 返回错误。
func (a *app) tryAdopt(ctx context.Context, schema string, ver int64) (bool, error) {
	needsAdoption, err := a.checkAdoption(ctx, schema)
	if err != nil {
		return false, fmt.Errorf("[%s] %w", schema, err)
	}
	if !needsAdoption {
		return false, nil
	}
	if err := a.adopt(ctx, schema, ver); err != nil {
		return false, fmt.Errorf("[%s] 接管失败: %w", schema, err)
	}
	return true, nil
}

// checkAdoption 只读检查无版本记录的租户是否满足接管条件。
func (a *app) checkAdoption(ctx context.Context, schema string) (bool, error) {
	ok, err := a.hasVersion(ctx, schema)
	if err != nil || ok {
		return false, err
	}
	diffs, err := a.columnDiff(ctx, schema)
	if err != nil {
		return false, err
	}
	if len(diffs) > 0 {
		printDiffs(schema, diffs)
		return false, fmt.Errorf("无版本记录，且结构与 %s 不一致，无法接管", baseSchema)
	}
	return true, nil
}

// markBase：为 tenant_1 建版本表，并写入版本 0、1（与 goose 自己建表时的行为一致）。
func (a *app) markBase(ctx context.Context) error {
	s := quoteIdent(baseSchema)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
			id         integer PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
			version_id bigint    NOT NULL,
			is_applied boolean   NOT NULL,
			tstamp     timestamp NOT NULL DEFAULT now())`, s, versionTable),
		fmt.Sprintf(`DELETE FROM %s.%s`, s, versionTable),
		fmt.Sprintf(`INSERT INTO %s.%s (version_id, is_applied) VALUES (0, true), (1, true)`, s, versionTable),
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// adopt：给 schema 写入版本记录，内容复制自 tenant_1 中 version_id <= upTo 的记录。
// 只在 schema 没有任何 version_id > 0 的记录时调用。
func (a *app) adopt(ctx context.Context, schema string, upTo int64) error {
	s, b := quoteIdent(schema), quoteIdent(baseSchema)
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	stmts := []string{
		// 复制脚本可能已建了空表，也可能没建；LIKE ... INCLUDING ALL 会给新表单独建 identity 序列
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (LIKE %s.%s INCLUDING ALL)`, s, versionTable, b, versionTable),
		fmt.Sprintf(`DELETE FROM %s.%s`, s, versionTable),
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s.%s (version_id, is_applied, tstamp)
		SELECT version_id, is_applied, tstamp FROM %s.%s
		WHERE version_id <= $1 ORDER BY id`, s, versionTable, b, versionTable), upTo); err != nil {
		return err
	}
	return tx.Commit()
}

// ---------- 迁移单个 schema ----------

func (a *app) migrateSchema(ctx context.Context, schema string) error {
	db, err := a.openSchema(schema)
	if err != nil {
		return err
	}
	defer db.Close()

	var opts []goose.ProviderOption
	if *flagAllowMissing {
		opts = append(opts, goose.WithAllowOutofOrder(true))
	}
	p, err := goose.NewProvider(goose.DialectPostgres, db, a.fsys, opts...)
	if err != nil {
		return err
	}
	results, err := p.Up(ctx)
	for _, r := range results {
		fmt.Printf("[%s] 已执行 %s (%s)\n", schema, filepath.Base(r.Source.Path), r.Duration)
	}
	if err != nil {
		return err
	}
	if len(results) == 0 {
		fmt.Printf("[%s] 已是最新\n", schema)
	}
	return nil
}

// openSchema：打开一个 search_path 指向该租户的连接。
// 迁移文件里不写 schema 名，全部靠 search_path 落到对应租户。
func (a *app) openSchema(schema string) (*sql.DB, error) {
	cfg, err := pgx.ParseConfig(a.dsn)
	if err != nil {
		return nil, err
	}
	if cfg.RuntimeParams == nil {
		cfg.RuntimeParams = map[string]string{}
	}
	cfg.RuntimeParams["search_path"] = schema + ",public"
	cfg.RuntimeParams["lock_timeout"] = *flagLockTimeout
	db := stdlib.OpenDB(*cfg)
	db.SetMaxOpenConns(1)
	return db, nil
}

// ---------- 元数据查询 ----------

func (a *app) tenants(ctx context.Context) ([]string, error) {
	rows, err := a.db.QueryContext(ctx,
		`SELECT nspname FROM pg_namespace WHERE nspname LIKE 'tenant\_%' ORDER BY nspname`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	hasBase := false
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		if !schemaNameRe.MatchString(s) {
			return nil, fmt.Errorf("schema 名不符合约定: %q", s)
		}
		if s == baseSchema {
			hasBase = true
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if !hasBase {
		return nil, fmt.Errorf("找不到 %s", baseSchema)
	}
	return out, nil
}

// hasVersion：schema 中是否存在 version_id > 0 的版本记录。
func (a *app) hasVersion(ctx context.Context, schema string) (bool, error) {
	var exists bool
	if err := a.db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`,
		quoteIdent(schema)+"."+versionTable).Scan(&exists); err != nil || !exists {
		return false, err
	}
	var n int
	err := a.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT count(*) FROM %s.%s WHERE version_id > 0`, quoteIdent(schema), versionTable)).Scan(&n)
	return n > 0, err
}

func (a *app) currentVersion(ctx context.Context, schema string) (int64, error) {
	var v int64
	err := a.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT coalesce(max(version_id), 0) FROM %s.%s WHERE is_applied`,
		quoteIdent(schema), versionTable)).Scan(&v)
	return v, err
}

type diffRow struct{ Kind, Table, Name, Problem string }

// columnDiff：字段比对（表、字段名、完整类型含长度、NOT NULL）。缺表会列出该表缺少的所有字段。
const columnDiffSQL = `
WITH a AS (
  SELECT c.relname AS tbl, att.attname AS col, format_type(att.atttypid, att.atttypmod) AS typ, att.attnotnull AS nn
  FROM pg_attribute att
  JOIN pg_class c ON c.oid = att.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $1 AND c.relkind IN ('r','p') AND att.attnum > 0 AND NOT att.attisdropped
    AND c.relname <> 'goose_db_version'
), b AS (
  SELECT c.relname AS tbl, att.attname AS col, format_type(att.atttypid, att.atttypmod) AS typ, att.attnotnull AS nn
  FROM pg_attribute att
  JOIN pg_class c ON c.oid = att.attrelid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = $2 AND c.relkind IN ('r','p') AND att.attnum > 0 AND NOT att.attisdropped
    AND c.relname <> 'goose_db_version'
)
SELECT '字段', coalesce(a.tbl, b.tbl), coalesce(a.col, b.col),
  format('期望=%s，实际=%s',
    CASE WHEN a.col IS NULL THEN '不应存在'
         ELSE a.typ || CASE WHEN a.nn THEN ' NOT NULL' ELSE ' NULL' END END,
    CASE WHEN b.col IS NULL THEN '缺少字段'
         ELSE b.typ || CASE WHEN b.nn THEN ' NOT NULL' ELSE ' NULL' END END)
FROM a FULL JOIN b ON a.tbl = b.tbl AND a.col = b.col
WHERE a.typ IS DISTINCT FROM b.typ OR a.nn IS DISTINCT FROM b.nn
ORDER BY 2, 3`

// indexDiff：按索引定义比对（去掉索引名和 schema 名，因为复制出来的索引名可能不同）。
const indexDiffSQL = `
WITH a AS (
  SELECT tablename AS tbl,
         regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX \S+ ON \S+\.', 'CREATE \1INDEX ON ') AS def
  FROM pg_indexes WHERE schemaname = $1 AND tablename <> 'goose_db_version'
), b AS (
  SELECT tablename AS tbl,
         regexp_replace(indexdef, '^CREATE (UNIQUE )?INDEX \S+ ON \S+\.', 'CREATE \1INDEX ON ') AS def
  FROM pg_indexes WHERE schemaname = $2 AND tablename <> 'goose_db_version'
)
SELECT '索引', coalesce(a.tbl, b.tbl), coalesce(a.def, b.def),
  CASE WHEN b.def IS NULL THEN 'MISSING' ELSE 'EXTRA' END
FROM a FULL JOIN b ON a.tbl = b.tbl AND a.def = b.def
WHERE a.def IS NULL OR b.def IS NULL
ORDER BY 2, 3`

func (a *app) columnDiff(ctx context.Context, schema string) ([]diffRow, error) {
	return a.queryDiff(ctx, columnDiffSQL, schema)
}

func (a *app) indexDiff(ctx context.Context, schema string) ([]diffRow, error) {
	return a.queryDiff(ctx, indexDiffSQL, schema)
}

func (a *app) queryDiff(ctx context.Context, q, schema string) ([]diffRow, error) {
	rows, err := a.db.QueryContext(ctx, q, baseSchema, schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []diffRow
	for rows.Next() {
		var d diffRow
		if err := rows.Scan(&d.Kind, &d.Table, &d.Name, &d.Problem); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// ---------- 工具函数 ----------

func (a *app) withGlobalLock(ctx context.Context, fn func(context.Context) error) error {
	conn, err := a.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	var ok bool
	if err := conn.QueryRowContext(ctx, `SELECT pg_try_advisory_lock(hashtext($1))`, globalLockKey).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return errors.New("另一个迁移进程正在运行")
	}
	defer conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock(hashtext($1))`, globalLockKey)
	return fn(ctx)
}

func (a *app) parallel(ctx context.Context, schemas []string, fn func(context.Context, string) error) map[string]error {
	var (
		mu     sync.Mutex
		wg     sync.WaitGroup
		failed = map[string]error{}
		sem    = make(chan struct{}, max(1, *flagConcurrency))
	)
	for _, s := range schemas {
		wg.Add(1)
		sem <- struct{}{}
		go func(s string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := fn(ctx, s); err != nil {
				fmt.Fprintf(os.Stderr, "[%s] 失败: %v\n", s, err)
				mu.Lock()
				failed[s] = err
				mu.Unlock()
			}
		}(s)
	}
	wg.Wait()
	return failed
}

func printDiffs(schema string, diffs []diffRow) {
	for _, d := range diffs {
		fmt.Printf("[%s] 表=%q，%s=%q，%s\n", schema, d.Table, d.Kind, d.Name, d.Problem)
	}
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
