package main

// 依赖 pg_dump 的两个命令：dump-baseline（生成 00001_baseline.sql）和 compare（完整结构比对）。
// pg_dump 的版本必须不低于数据库服务器的版本。Windows 上通常在
// C:\Program Files\PostgreSQL\<版本>\bin\pg_dump.exe，可加进 PATH，或用 -pg-dump 指定完整路径。

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	flagPgDump    = flag.String("pg-dump", "pg_dump", "pg_dump 可执行文件路径")
	flagOut       = flag.String("out", "", "dump-baseline：输出文件路径")
	flagExpectDSN = flag.String("expect-dsn", "", "compare：期望结构所在库的连接串，默认与 -dsn 相同")
)

// pgDump 导出一个 schema 的结构（不含数据、owner、权限、goose 版本表），并去掉 schema 前缀。
func pgDump(ctx context.Context, dsn, schema string) ([]string, error) {
	args := []string{
		"--schema-only", "--no-owner", "--no-privileges", "--no-comments",
		"-n", schema,
		"-T", schema + "." + versionTable,
		"-T", schema + "." + versionTable + "_id_seq",
		"--dbname=" + dsn,
	}
	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, *flagPgDump, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("执行 pg_dump 失败: %v\n%s", err, stderr.String())
	}

	prefix := regexp.MustCompile(`\b"?` + regexp.QuoteMeta(schema) + `"?\.`)
	var lines []string
	for _, l := range strings.Split(stdout.String(), "\n") {
		// Windows pg_dump 可能在本身含 CRLF 的函数体行尾再加一个 CR。
		l = strings.TrimRight(l, "\r")
		switch {
		case strings.HasPrefix(l, `\restrict `), strings.HasPrefix(l, `\unrestrict `):
			continue // 新版 pg_dump 的 psql 元命令，goose 不认识
		case strings.HasPrefix(l, "SELECT pg_catalog.set_config('search_path'"):
			continue // 会清空 search_path，迁移就落不到租户里了
		case strings.HasPrefix(l, "CREATE SCHEMA "):
			continue
		}
		if !strings.HasPrefix(l, "-- Name: ") { // 注释头里保留原 schema 名，后面按它切分对象
			l = prefix.ReplaceAllString(l, "")
		}
		lines = append(lines, l)
	}
	return lines, nil
}

// dumpBaseline 生成 00001_baseline.sql。
func (a *app) dumpBaseline(ctx context.Context) error {
	if *flagOut == "" {
		return errors.New("请用 -out 指定输出文件，如 -out migrations/tenant/00001_baseline.sql")
	}
	if _, err := os.Stat(*flagOut); err == nil {
		return fmt.Errorf("%s 已存在；上线后的 baseline 不能覆盖，确实要重新生成请先手动删除", *flagOut)
	}
	lines, err := pgDump(ctx, a.dsn, baseSchema)
	if err != nil {
		return err
	}
	var b strings.Builder
	fmt.Fprintf(&b, "-- 由 migrate dump-baseline 从 %s 生成，时间 %s\n", baseSchema, time.Now().Format("2006-01-02 15:04:05"))
	b.WriteString("-- 现有租户只标记、不执行这个文件（migrate baseline）；只有空 schema 从零建表时才会执行。\n\n")
	b.WriteString("-- +goose Up\n-- +goose StatementBegin\n")
	for _, l := range lines {
		b.WriteString(l + "\n")
	}
	b.WriteString("-- +goose StatementEnd\n\n-- +goose Down\n-- 基线不提供回退\n")
	if err := os.WriteFile(*flagOut, []byte(b.String()), 0o644); err != nil { // UTF-8，无 BOM
		return err
	}
	fmt.Printf("已生成 %s（%d 行）\n", *flagOut, len(lines))
	return nil
}

// dumpObject 是 pg_dump 输出里的一个对象：表、序列、索引、约束、函数、触发器……
type dumpObject struct {
	Key  string // 如 "TABLE orders"、"INDEX idx_orders_user"
	Body []string
}

var nameHeader = regexp.MustCompile(`^-- Name: (.+?); Type: (.+?); Schema:`)

const dumpIdentifier = `(?:"(?:[^"]|"")*"|[A-Za-z_][A-Za-z_0-9$]*)`

var (
	indexName      = regexp.MustCompile(`(?m)^(CREATE (?:UNIQUE )?INDEX )` + dumpIdentifier + `( ON )`)
	constraintName = regexp.MustCompile(`(?m)^(\s*ADD CONSTRAINT )` + dumpIdentifier + `( )`)
	identityName   = regexp.MustCompile(`(?m)^(\s*SEQUENCE NAME )` + dumpIdentifier + `(\s*$)`)
)

func (o *dumpObject) renameSignature() string {
	objectType, _, _ := strings.Cut(o.Key, " ")
	definition := strings.Join(o.Body, "\n")
	var declaration *regexp.Regexp
	switch {
	case objectType == "INDEX":
		declaration = indexName
	case strings.HasPrefix(o.Key, "CONSTRAINT "), strings.HasPrefix(o.Key, "FK CONSTRAINT "):
		declaration = constraintName
	case objectType == "SEQUENCE" && strings.Contains(definition, " AS IDENTITY ("):
		declaration = identityName
	default:
		return ""
	}
	// 只忽略声明里的对象名。表名、索引表达式、外键引用和序列参数都参与匹配。
	if !declaration.MatchString(definition) {
		return ""
	}
	return objectType + "\n" + declaration.ReplaceAllString(definition, "${1}<name>${2}")
}

// splitObjects 按 pg_dump 的 "-- Name: ...; Type: ...;" 注释头切分对象，丢掉注释、空行和 SET 语句。
func splitObjects(lines []string) map[string]*dumpObject {
	objs := map[string]*dumpObject{}
	var cur *dumpObject
	for _, l := range lines {
		if m := nameHeader.FindStringSubmatch(l); m != nil {
			if m[2] == "SCHEMA" { // schema 本身，名字必然不同，不比较
				cur = nil
				continue
			}
			key := m[2] + " " + m[1]
			cur = &dumpObject{Key: key}
			for i := 2; objs[cur.Key] != nil; i++ { // 同名对象（如函数重载）
				cur.Key = fmt.Sprintf("%s #%d", key, i)
			}
			objs[cur.Key] = cur
			continue
		}
		if cur == nil || strings.HasPrefix(l, "--") || strings.TrimSpace(l) == "" ||
			strings.HasPrefix(l, "SET ") || strings.HasPrefix(l, "SELECT pg_catalog.set_config") {
			continue
		}
		cur.Body = append(cur.Body, l)
	}
	return objs
}

// compare：完整比较两个 schema 的结构，用法 compare <期望schema> <实际schema>。
func (a *app) compare(ctx context.Context, args []string) error {
	if len(args) != 2 {
		return errors.New("用法: migrate compare <期望schema> <实际schema>，例如 compare tenant_1 tenant_5")
	}
	expSchema, actSchema := args[0], args[1]
	expDSN := *flagExpectDSN
	if expDSN == "" {
		expDSN = a.dsn
	}
	expLines, err := pgDump(ctx, expDSN, expSchema)
	if err != nil {
		return err
	}
	actLines, err := pgDump(ctx, a.dsn, actSchema)
	if err != nil {
		return err
	}
	exp, act := splitObjects(expLines), splitObjects(actLines)

	keys := map[string]bool{}
	for k := range exp {
		keys[k] = true
	}
	for k := range act {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)

	actualByDefinition := make(map[string][]string)
	for _, key := range sorted {
		if exp[key] == nil {
			if signature := act[key].renameSignature(); signature != "" {
				actualByDefinition[signature] = append(actualByDefinition[signature], key)
			}
		}
	}
	renamed := make(map[string]string)
	matchedActual := make(map[string]bool)
	for _, key := range sorted {
		if act[key] == nil {
			signature := exp[key].renameSignature()
			if candidates := actualByDefinition[signature]; len(candidates) > 0 {
				renamed[key] = candidates[0]
				matchedActual[candidates[0]] = true
				actualByDefinition[signature] = candidates[1:]
			}
		}
	}

	diffs := 0
	for _, k := range sorted {
		ev, av := exp[k], act[k]
		switch {
		case matchedActual[k]:
			continue
		case renamed[k] != "":
			diffs++
			fmt.Printf("\n[名称不同] 期望 %s，实际 %s\n", k, renamed[k])
		case av == nil:
			diffs++
			fmt.Printf("\n[缺少] %s\n", k)
			for _, l := range ev.Body {
				fmt.Println("  - " + l)
			}
		case ev == nil:
			diffs++
			fmt.Printf("\n[多出] %s\n", k)
			for _, l := range av.Body {
				fmt.Println("  + " + l)
			}
		case strings.Join(ev.Body, "\n") != strings.Join(av.Body, "\n"):
			diffs++
			fmt.Printf("\n[不同] %s   （- %s，+ %s）\n", k, expSchema, actSchema)
			for _, l := range lineDiff(ev.Body, av.Body) {
				fmt.Println("  " + l)
			}
		}
	}
	if diffs > 0 {
		return fmt.Errorf("%s 与 %s 有 %d 处不同", actSchema, expSchema, diffs)
	}
	fmt.Printf("%s 与 %s 结构完全一致（%d 个对象）\n", actSchema, expSchema, len(exp))
	return nil
}

// lineDiff：两段文本的逐行差异（LCS），只输出变化的行。每个对象只有几十行，直接用 O(n*m)。
func lineDiff(a, b []string) []string {
	n, m := len(a), len(b)
	lcs := make([][]int, n+1)
	for i := range lcs {
		lcs[i] = make([]int, m+1)
	}
	for i := n - 1; i >= 0; i-- {
		for j := m - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else {
				lcs[i][j] = max(lcs[i+1][j], lcs[i][j+1])
			}
		}
	}
	var out []string
	i, j := 0, 0
	for i < n && j < m {
		switch {
		case a[i] == b[j]:
			i, j = i+1, j+1
		case lcs[i+1][j] >= lcs[i][j+1]:
			out = append(out, "- "+a[i])
			i++
		default:
			out = append(out, "+ "+b[j])
			j++
		}
	}
	for ; i < n; i++ {
		out = append(out, "- "+a[i])
	}
	for ; j < m; j++ {
		out = append(out, "+ "+b[j])
	}
	return out
}
