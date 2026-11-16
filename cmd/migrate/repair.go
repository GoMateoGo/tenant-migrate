package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"regexp"
	"sort"
	"strings"
)

type baselineTable struct {
	columns map[string]string
	checks  []string
}

type baselineStatement struct {
	table, sequence, sql string
}

var (
	createBaselineTable    = regexp.MustCompile(`^CREATE (?:UNLOGGED )?TABLE (` + dumpIdentifier + `) \(`)
	alterBaselineTable     = regexp.MustCompile(`^ALTER TABLE (?:ONLY )?(` + dumpIdentifier + `)\s`)
	baselineIndexTable     = regexp.MustCompile(`^CREATE (?:UNIQUE )?INDEX ` + dumpIdentifier + ` ON (?:ONLY )?(` + dumpIdentifier + `)\s`)
	baselineTriggerTable   = regexp.MustCompile(`^CREATE (?:CONSTRAINT )?TRIGGER .*? ON (` + dumpIdentifier + `)\s`)
	createBaselineSequence = regexp.MustCompile(`^CREATE (?:UNLOGGED )?SEQUENCE (` + dumpIdentifier + `)\s`)
	ownedBaselineSequence  = regexp.MustCompile(`^ALTER SEQUENCE (` + dumpIdentifier + `) OWNED BY (` + dumpIdentifier + `)\.`)
	baselineColumnLine     = regexp.MustCompile(`^    (` + dumpIdentifier + `) (.+)$`)
	baselineAddDefault     = regexp.MustCompile(`^ALTER TABLE (?:ONLY )?` + dumpIdentifier + ` ALTER COLUMN (` + dumpIdentifier + `) SET DEFAULT (.+);$`)
	simpleColumnType       = regexp.MustCompile(`^(?:character varying|character|varchar|char|text|smallint|integer|bigint|boolean|numeric|decimal|real|double precision|date|time(?:stamp)?(?:\([0-9]+\))? (?:with|without) time zone|uuid|jsonb?|bytea)(?:\([0-9]+(?:,\s*[0-9]+)?\))?$`)
	literalColumnDefault   = regexp.MustCompile(`^(?:'(?:[^']|'')*'|[-+]?[0-9]+(?:\.[0-9]+)?|true|false|NULL)$`)
	sqlIdentifierToken     = regexp.MustCompile(dumpIdentifier)
)

func (a *app) repair(ctx context.Context, args []string) error {
	if len(args) != 1 || !schemaNameRe.MatchString(args[0]) {
		return fmt.Errorf("用法: migrate [-dir 迁移目录] repair <租户schema>，例如 tenant_34")
	}
	schema := args[0]
	baseline, err := fs.ReadFile(a.fsys, "00001_baseline.sql")
	if err != nil {
		return fmt.Errorf("读取 %s/00001_baseline.sql: %w", a.dir, err)
	}
	tables, statements, err := readRepairBaseline(string(baseline))
	if err != nil {
		return err
	}
	if registered, err := a.hasVersion(ctx, schema); err != nil {
		return err
	} else if registered {
		return fmt.Errorf("[%s] 已登记迁移版本，基线补齐仅用于尚未登记的租户", schema)
	}

	var database string
	var schemaExists bool
	if err := a.db.QueryRowContext(ctx, `SELECT current_database(), EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = $1)`, schema).Scan(&database, &schemaExists); err != nil {
		return err
	}
	if !schemaExists {
		return fmt.Errorf("找不到 schema %s", schema)
	}
	rows, err := a.db.QueryContext(ctx, `
SELECT c.relname, c.relkind = 'r' AND NOT c.relispartition, coalesce(a.attname, '')
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
WHERE n.nspname = $1 AND c.relkind IN ('r', 'p', 'f', 'v', 'm') AND c.relname <> 'goose_db_version'`, schema)
	if err != nil {
		return err
	}
	defer rows.Close()
	existing := make(map[string]map[string]bool)
	for rows.Next() {
		var table, column string
		var ordinary bool
		if err := rows.Scan(&table, &ordinary, &column); err != nil {
			return err
		}
		if tables[table] == nil {
			continue
		}
		if !ordinary {
			return fmt.Errorf("[%s] %s 不是普通表，需要人工处理", schema, table)
		}
		if existing[table] == nil {
			existing[table] = make(map[string]bool)
		}
		existing[table][column] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}

	missingTables := make(map[string]bool)
	var missingColumns, blocked []string
	for table, definition := range tables {
		columns := existing[table]
		if columns == nil {
			missingTables[table] = true
			continue
		}
		for column, declaration := range definition.columns {
			if columns[column] {
				continue
			}
			qualified := quoteIdent(schema) + "." + quoteIdent(table)
			if !isSimpleRepairColumn(declaration) || baselineMentionsColumn(table, column, definition.checks, statements) {
				blocked = append(blocked, qualified+"."+quoteIdent(column))
				continue
			}
			missingColumns = append(missingColumns, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s;", qualified, quoteIdent(column), declaration))
		}
	}
	if len(blocked) > 0 {
		sort.Strings(blocked)
		return fmt.Errorf("以下缺列含复杂定义或依赖，请人工处理: %s", strings.Join(blocked, ", "))
	}
	if len(missingTables)+len(missingColumns) == 0 {
		fmt.Printf("-- [%s] 没有需要补齐的表或字段。\n", schema)
		return nil
	}

	sequences := make(map[string]bool)
	for _, statement := range statements {
		if missingTables[statement.table] && statement.sequence != "" {
			sequences[statement.sequence] = true
		}
	}
	// 补表还需要基线中通过默认值引用、没有 OWNED BY 的普通序列。
	for _, statement := range statements {
		if statement.table != "" || statement.sequence == "" {
			continue
		}
		for _, tableStatement := range statements {
			if missingTables[tableStatement.table] && mentionsSQLIdentifier(tableStatement.sql, statement.sequence) {
				sequences[statement.sequence] = true
				break
			}
		}
	}

	var plan strings.Builder
	fmt.Fprintf(&plan, "-- 目标库 %s，租户 %s。审阅后在目标库执行整段 SQL。\n", quoteSQLString(database), schema)
	fmt.Fprintln(&plan, "BEGIN;\nSET LOCAL standard_conforming_strings = on;")
	fmt.Fprintf(&plan, "SET LOCAL lock_timeout = %s;\n", quoteSQLString(*flagLockTimeout))
	fmt.Fprintf(&plan, "SET LOCAL search_path = %s, pg_temp;\n", quoteIdent(schema))
	fmt.Fprintf(&plan, "SELECT pg_advisory_xact_lock(hashtext(%s));\n", quoteSQLString(globalLockKey))
	fmt.Fprintf(&plan, `DO $repair$
BEGIN
  IF current_database() <> %s OR NOT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = %s) THEN
    RAISE EXCEPTION '目标数据库或租户 schema 与修复计划不一致';
  END IF;
  IF to_regclass(%s) IS NOT NULL THEN
    IF EXISTS (SELECT 1 FROM %s.%s WHERE version_id > 0) THEN
      RAISE EXCEPTION '租户已登记迁移版本，请重新核对修复计划';
    END IF;
  END IF;
END;
$repair$;
`, quoteSQLString(database), quoteSQLString(schema), quoteSQLString(quoteIdent(schema)+"."+quoteIdent(versionTable)), quoteIdent(schema), quoteIdent(versionTable))
	sort.Strings(missingColumns)
	for _, statement := range missingColumns {
		fmt.Fprintln(&plan, statement)
	}
	for _, statement := range statements {
		if missingTables[statement.table] || (statement.table == "" && sequences[statement.sequence]) {
			fmt.Fprintln(&plan, statement.sql)
		}
	}
	fmt.Fprintln(&plan, "COMMIT;")
	_, err = fmt.Fprint(os.Stdout, plan.String())
	return err
}

func readRepairBaseline(source string) (map[string]*baselineTable, []baselineStatement, error) {
	tables := make(map[string]*baselineTable)
	var statements []baselineStatement
	var kind string
	var block []string
	flush := func() error {
		if len(block) == 0 {
			return nil
		}
		body := strings.Join(block, "\n")
		switch kind {
		case "TABLE":
			match := createBaselineTable.FindStringSubmatch(body)
			if match == nil || !strings.HasSuffix(body, "\n);") {
				return fmt.Errorf("repair 仅支持基线中的普通表: %s", block[0])
			}
			table := &baselineTable{columns: make(map[string]string)}
			for _, line := range block[1 : len(block)-1] {
				field := baselineColumnLine.FindStringSubmatch(line)
				if field == nil {
					return fmt.Errorf("无法读取基线表 %s 的字段定义", match[1])
				}
				switch field[1] {
				case "CONSTRAINT", "CHECK", "PRIMARY", "UNIQUE", "FOREIGN", "EXCLUDE":
					table.checks = append(table.checks, line)
				default:
					table.columns[unquoteSQLIdentifier(field[1])] = strings.TrimSuffix(field[2], ",")
				}
			}
			name := unquoteSQLIdentifier(match[1])
			tables[name] = table
			statements = append(statements, baselineStatement{table: name, sql: body})
		case "DEFAULT", "CONSTRAINT", "FK CONSTRAINT", "INDEX", "TRIGGER", "SEQUENCE", "SEQUENCE OWNED BY":
			statement := baselineStatement{sql: body}
			switch {
			case kind == "SEQUENCE OWNED BY":
				if match := ownedBaselineSequence.FindStringSubmatch(body); match != nil {
					statement.sequence, statement.table = unquoteSQLIdentifier(match[1]), unquoteSQLIdentifier(match[2])
				}
			case kind == "SEQUENCE":
				if match := createBaselineSequence.FindStringSubmatch(body); match != nil {
					statement.sequence = unquoteSQLIdentifier(match[1])
				} else if match := alterBaselineTable.FindStringSubmatch(body); match != nil {
					statement.table = unquoteSQLIdentifier(match[1])
				}
			default:
				pattern := alterBaselineTable
				if kind == "INDEX" {
					pattern = baselineIndexTable
				} else if kind == "TRIGGER" {
					pattern = baselineTriggerTable
				}
				if match := pattern.FindStringSubmatch(body); match != nil {
					statement.table = unquoteSQLIdentifier(match[1])
				}
			}
			if statement.table == "" && statement.sequence == "" {
				return fmt.Errorf("无法识别基线中的 %s 定义: %s", kind, block[0])
			}
			statements = append(statements, statement)
		}
		return nil
	}

	// 按本工具生成的 pg_dump 对象头读取 Up 段，保留依赖对象在基线里的顺序。
	for _, line := range strings.Split(source, "\n") {
		line = strings.TrimRight(line, "\r")
		if line == "-- +goose Down" {
			break
		}
		if match := nameHeader.FindStringSubmatch(line); match != nil {
			if err := flush(); err != nil {
				return nil, nil, err
			}
			kind, block = match[2], nil
			continue
		}
		if kind == "" || strings.TrimSpace(line) == "" || strings.HasPrefix(line, "--") || strings.HasPrefix(line, "SET ") {
			continue
		}
		block = append(block, line)
	}
	if err := flush(); err != nil {
		return nil, nil, err
	}
	if len(tables) == 0 {
		return nil, nil, fmt.Errorf("00001_baseline.sql 未找到表定义，请使用 dump-baseline 生成的基线")
	}
	for _, statement := range statements {
		if match := baselineAddDefault.FindStringSubmatch(statement.sql); match != nil {
			if table := tables[statement.table]; table != nil {
				column := unquoteSQLIdentifier(match[1])
				table.columns[column] += " DEFAULT " + match[2]
			}
		}
	}
	return tables, statements, nil
}

func isSimpleRepairColumn(declaration string) bool {
	typeName, defaultSQL, hasDefault := strings.Cut(declaration, " DEFAULT ")
	if !simpleColumnType.MatchString(typeName) {
		return false
	}
	return !hasDefault || literalColumnDefault.MatchString(strings.TrimSuffix(defaultSQL, "::"+typeName))
}

func baselineMentionsColumn(table, column string, checks []string, statements []baselineStatement) bool {
	for _, check := range checks {
		if mentionsSQLIdentifier(check, column) {
			return true
		}
	}
	for _, statement := range statements {
		if createBaselineTable.MatchString(statement.sql) || baselineAddDefault.MatchString(statement.sql) {
			continue
		}
		if (statement.table == table || strings.Contains(statement.sql, "REFERENCES "+quoteIdent(table)+"(") ||
			strings.Contains(statement.sql, "REFERENCES "+table+"(")) && mentionsSQLIdentifier(statement.sql, column) {
			return true
		}
	}
	return false
}

func mentionsSQLIdentifier(statement, name string) bool {
	for _, token := range sqlIdentifierToken.FindAllString(statement, -1) {
		if unquoteSQLIdentifier(token) == name {
			return true
		}
	}
	return false
}

func unquoteSQLIdentifier(name string) string {
	if strings.HasPrefix(name, `"`) {
		return strings.ReplaceAll(name[1:len(name)-1], `""`, `"`)
	}
	return name
}

func quoteSQLString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
