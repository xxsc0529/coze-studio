/*
 * Copyright 2025 coze-dev Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package oceanbase

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"gorm.io/gorm"

	"github.com/coze-dev/coze-studio/backend/infra/contract/es"
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
)

type oceanbaseSearchClient struct {
	db *gorm.DB
}

func NewOceanBaseSearchClient(db *gorm.DB) es.Client {
	return &oceanbaseSearchClient{db: db}
}

func (o *oceanbaseSearchClient) Search(ctx context.Context, index string, req *es.Request) (*es.Response, error) {
	// 根据索引名称确定查询的表
	var tableName string
	switch index {
	case "project_search", "project_draft":
		tableName = "project_search"
	case "resource_search":
		tableName = "resource_search"
	default:
		// 默认返回空结果
		return &es.Response{
			Hits: es.HitsMetadata{
				Hits: []es.Hit{},
				Total: &es.TotalHits{
					Value: 0,
				},
			},
		}, nil
	}

	// 构建SQL查询
	sqlQuery, args := o.buildSQLQuery(tableName, req)

	logs.CtxDebugf(ctx, "[OceanBase Search] SQL: %s, Args: %v", sqlQuery, args)

	// 执行查询
	var results []map[string]interface{}
	if err := o.db.Raw(sqlQuery, args...).Scan(&results).Error; err != nil {
		return nil, fmt.Errorf("oceanbase search failed: %w", err)
	}

	// 转换为ES响应格式
	hits := make([]es.Hit, 0, len(results))
	for _, result := range results {
		// 将结果转换为JSON
		source, err := json.Marshal(result)
		if err != nil {
			continue
		}

		// 获取ID
		var id string
		if idVal, ok := result["id"]; ok {
			switch v := idVal.(type) {
			case int64:
				id = strconv.FormatInt(v, 10)
			case string:
				id = v
			default:
				id = fmt.Sprintf("%v", v)
			}
		}

		hits = append(hits, es.Hit{
			Id_:     &id,
			Score_:  nil, // OceanBase不支持评分
			Source_: source,
		})
	}

	return &es.Response{
		Hits: es.HitsMetadata{
			Hits: hits,
			Total: &es.TotalHits{
				Value: int64(len(hits)),
			},
		},
	}, nil
}

func (o *oceanbaseSearchClient) buildSQLQuery(tableName string, req *es.Request) (string, []interface{}) {
	var args []interface{}

	// 基础查询
	sql := fmt.Sprintf("SELECT * FROM %s WHERE 1=1", tableName)

	// 处理查询条件
	if req.Query != nil && req.Query.Bool != nil {
		sql, args = o.processBoolQuery(sql, args, req.Query.Bool)
	}

	// 处理排序
	if len(req.Sort) > 0 {
		var sortClauses []string
		for _, sort := range req.Sort {
			order := "ASC"
			if !sort.Asc {
				order = "DESC"
			}
			sortClauses = append(sortClauses, fmt.Sprintf("%s %s", sort.Field, order))
		}
		sql += " ORDER BY " + strings.Join(sortClauses, ", ")
	}

	// 处理分页
	if req.Size != nil {
		sql += fmt.Sprintf(" LIMIT %d", *req.Size)
	}

	if req.From != nil {
		sql += fmt.Sprintf(" OFFSET %d", *req.From)
	}

	return sql, args
}

func (o *oceanbaseSearchClient) processBoolQuery(sql string, args []interface{}, boolQuery *es.BoolQuery) (string, []interface{}) {
	// 处理Must条件
	for _, query := range boolQuery.Must {
		sql, args = o.processQuery(sql, args, &query, "AND")
	}

	// 处理Filter条件
	for _, query := range boolQuery.Filter {
		sql, args = o.processQuery(sql, args, &query, "AND")
	}

	// 处理Should条件
	if len(boolQuery.Should) > 0 {
		var shouldClauses []string
		var shouldArgs []interface{}

		for _, query := range boolQuery.Should {
			clause, clauseArgs := o.processQuery("", shouldArgs, &query, "")
			if clause != "" {
				shouldClauses = append(shouldClauses, strings.TrimPrefix(clause, "AND "))
				shouldArgs = append(shouldArgs, clauseArgs...)
			}
		}

		if len(shouldClauses) > 0 {
			sql += " AND (" + strings.Join(shouldClauses, " OR ") + ")"
			args = append(args, shouldArgs...)
		}
	}

	// 处理MustNot条件
	for _, query := range boolQuery.MustNot {
		sql, args = o.processQuery(sql, args, &query, "AND NOT")
	}

	return sql, args
}

func (o *oceanbaseSearchClient) processQuery(sql string, args []interface{}, query *es.Query, operator string) (string, []interface{}) {
	switch query.Type {
	case es.QueryTypeEqual:
		clause := fmt.Sprintf("%s %s = ?", operator, query.KV.Key)
		if sql == "" {
			clause = fmt.Sprintf("%s = ?", query.KV.Key)
		}
		return sql + " " + clause, append(args, query.KV.Value)

	case es.QueryTypeMatch:
		// 使用LIKE进行模糊匹配
		clause := fmt.Sprintf("%s %s LIKE ?", operator, query.KV.Key)
		if sql == "" {
			clause = fmt.Sprintf("%s LIKE ?", query.KV.Key)
		}
		return sql + " " + clause, append(args, "%"+fmt.Sprint(query.KV.Value)+"%")

	case es.QueryTypeContains:
		// 使用LIKE进行包含匹配
		clause := fmt.Sprintf("%s %s LIKE ?", operator, query.KV.Key)
		if sql == "" {
			clause = fmt.Sprintf("%s LIKE ?", query.KV.Key)
		}
		return sql + " " + clause, append(args, "%"+fmt.Sprint(query.KV.Value)+"%")

	case es.QueryTypeIn:
		// 处理IN查询
		if values, ok := query.KV.Value.([]interface{}); ok {
			placeholders := make([]string, len(values))
			for i := range values {
				placeholders[i] = "?"
			}
			clause := fmt.Sprintf("%s %s IN (%s)", operator, query.KV.Key, strings.Join(placeholders, ","))
			if sql == "" {
				clause = fmt.Sprintf("%s IN (%s)", query.KV.Key, strings.Join(placeholders, ","))
			}
			return sql + " " + clause, append(args, values...)
		}

	case es.QueryTypeNotExists:
		clause := fmt.Sprintf("%s %s IS NULL", operator, query.KV.Key)
		if sql == "" {
			clause = fmt.Sprintf("%s IS NULL", query.KV.Key)
		}
		return sql + " " + clause, args
	}

	return sql, args
}

func (o *oceanbaseSearchClient) Create(ctx context.Context, index string, id string, doc interface{}) error {
	// 根据索引名称确定插入的表
	var tableName string
	switch index {
	case "project_search", "project_draft":
		tableName = "project_search"
	case "resource_search":
		tableName = "resource_search"
	default:
		return nil
	}

	// 将文档转换为map
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	var docMap map[string]interface{}
	if err := json.Unmarshal(docBytes, &docMap); err != nil {
		return err
	}

	// 构建INSERT语句
	columns := make([]string, 0, len(docMap))
	values := make([]interface{}, 0, len(docMap))

	for col, val := range docMap {
		columns = append(columns, col)
		values = append(values, val)
	}

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON DUPLICATE KEY UPDATE ",
		tableName, strings.Join(columns, ","), strings.Repeat("?,", len(values)-1)+"?")

	// 构建UPDATE部分
	var updates []string
	for _, col := range columns {
		updates = append(updates, fmt.Sprintf("%s=VALUES(%s)", col, col))
	}
	sql += strings.Join(updates, ",")

	// 执行插入
	return o.db.Exec(sql, values...).Error
}

func (o *oceanbaseSearchClient) Update(ctx context.Context, index string, id string, doc interface{}) error {
	// 根据索引名称确定更新的表
	var tableName string
	switch index {
	case "project_search", "project_draft":
		tableName = "project_search"
	case "resource_search":
		tableName = "resource_search"
	default:
		return nil
	}

	// 将文档转换为map
	docBytes, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	var docMap map[string]interface{}
	if err := json.Unmarshal(docBytes, &docMap); err != nil {
		return err
	}

	// 构建UPDATE语句
	var sets []string
	var values []interface{}

	for col, val := range docMap {
		sets = append(sets, fmt.Sprintf("%s=?", col))
		values = append(values, val)
	}

	values = append(values, id) // WHERE条件的值

	sql := fmt.Sprintf("UPDATE %s SET %s WHERE id=?", tableName, strings.Join(sets, ","))

	// 执行更新
	return o.db.Exec(sql, values...).Error
}

func (o *oceanbaseSearchClient) Delete(ctx context.Context, index string, id string) error {
	// 根据索引名称确定删除的表
	var tableName string
	switch index {
	case "project_search", "project_draft":
		tableName = "project_search"
	case "resource_search":
		tableName = "resource_search"
	default:
		return nil
	}

	sql := fmt.Sprintf("DELETE FROM %s WHERE id=?", tableName)
	return o.db.Exec(sql, id).Error
}

func (o *oceanbaseSearchClient) Exists(ctx context.Context, index string) (bool, error) {
	// 检查表是否存在
	var count int64
	sql := "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?"
	err := o.db.Raw(sql, index).Count(&count).Error
	return count > 0, err
}

func (o *oceanbaseSearchClient) CreateIndex(ctx context.Context, index string, properties map[string]any) error {
	// OceanBase的表结构已经在初始化时创建，这里不需要做任何事情
	return nil
}

func (o *oceanbaseSearchClient) DeleteIndex(ctx context.Context, index string) error {
	// 删除表
	sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", index)
	return o.db.Exec(sql).Error
}

func (o *oceanbaseSearchClient) Types() es.Types {
	return &oceanbaseTypes{}
}

func (o *oceanbaseSearchClient) NewBulkIndexer(index string) (es.BulkIndexer, error) {
	return &oceanbaseBulkIndexer{
		db:    o.db,
		index: index,
	}, nil
}

type oceanbaseTypes struct{}

func (t *oceanbaseTypes) NewLongNumberProperty() any {
	return map[string]string{"type": "long"}
}

func (t *oceanbaseTypes) NewTextProperty() any {
	return map[string]string{"type": "text"}
}

func (t *oceanbaseTypes) NewUnsignedLongNumberProperty() any {
	return map[string]string{"type": "unsigned_long"}
}

type oceanbaseBulkIndexer struct {
	db    *gorm.DB
	index string
}

func (b *oceanbaseBulkIndexer) Add(ctx context.Context, item es.BulkIndexerItem) error {
	switch item.Action {
	case "index", "create":
		// 读取文档内容
		item.Body.Seek(0, 0)
		var doc interface{}
		if err := json.NewDecoder(item.Body).Decode(&doc); err != nil {
			return err
		}

		// 使用Create方法插入文档
		client := &oceanbaseSearchClient{db: b.db}
		return client.Create(ctx, b.index, item.DocumentID, doc)

	case "update":
		// 读取文档内容
		item.Body.Seek(0, 0)
		var doc interface{}
		if err := json.NewDecoder(item.Body).Decode(&doc); err != nil {
			return err
		}

		// 使用Update方法更新文档
		client := &oceanbaseSearchClient{db: b.db}
		return client.Update(ctx, b.index, item.DocumentID, doc)

	case "delete":
		// 使用Delete方法删除文档
		client := &oceanbaseSearchClient{db: b.db}
		return client.Delete(ctx, b.index, item.DocumentID)
	}

	return nil
}

func (b *oceanbaseBulkIndexer) Close(ctx context.Context) error {
	// 批量操作完成，不需要特殊处理
	return nil
}
