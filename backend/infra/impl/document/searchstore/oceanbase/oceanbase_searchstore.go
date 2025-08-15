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
	"fmt"
	"strings"

	"github.com/cloudwego/eino/components/indexer"
	"github.com/cloudwego/eino/components/retriever"
	"github.com/cloudwego/eino/schema"
	"gorm.io/gorm"

	"github.com/coze-dev/coze-studio/backend/infra/contract/document"
	"github.com/coze-dev/coze-studio/backend/infra/contract/document/searchstore"
	"github.com/coze-dev/coze-studio/backend/pkg/lang/ptr"
)

const (
	topK = 10
)

type oceanbaseSearchStore struct {
	config         *ManagerConfig
	collectionName string
	db             *gorm.DB
}

func (o *oceanbaseSearchStore) Retrieve(ctx context.Context, query string, opts ...retriever.Option) ([]*schema.Document, error) {
	options := retriever.GetCommonOptions(&retriever.Options{TopK: ptr.Of(topK)}, opts...)
	implSpecOptions := retriever.GetImplSpecificOptions(&searchstore.RetrieverOptions{}, opts...)

	// 根据搜索类型和查询内容智能选择搜索策略
	searchType := o.determineSearchType(query, implSpecOptions)

	switch searchType {
	case "hybrid":
		return o.hybridSearch(ctx, query, options, implSpecOptions)
	case "vector":
		return o.vectorSearch(ctx, query, options, implSpecOptions)
	case "fulltext":
		return o.fulltextSearch(ctx, query, options, implSpecOptions)
	default:
		return o.vectorSearch(ctx, query, options, implSpecOptions)
	}
}

func (o *oceanbaseSearchStore) determineSearchType(query string, implSpecOptions *searchstore.RetrieverOptions) string {
	// 检查是否有MultiMatch信息，如果有则根据MultiMatch判断搜索类型
	if implSpecOptions.MultiMatch != nil {
		// 如果有MultiMatch信息，优先使用混合搜索
		return "hybrid"
	}

	// 检查查询长度，短查询更适合向量搜索，长查询更适合全文搜索
	if len(query) < 10 {
		return "vector"
	} else if len(query) > 50 {
		return "fulltext"
	}

	// 默认使用混合搜索
	return "hybrid"
}

func (o *oceanbaseSearchStore) hybridSearch(ctx context.Context, query string, options *retriever.Options, implSpecOptions *searchstore.RetrieverOptions) ([]*schema.Document, error) {
	// 生成查询向量
	emb, err := o.config.Embedding.EmbedStrings(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("[hybridSearch] embed failed, %w", err)
	}
	if len(emb) != 1 {
		return nil, fmt.Errorf("[hybridSearch] unexpected embedding size, expected=1, got=%d", len(emb))
	}

	// 构建混合搜索SQL，结合向量搜索和全文搜索
	// 使用WITH子句分别进行向量搜索和全文搜索，然后合并结果
	tableName := o.getVectorTableName()
	sql := fmt.Sprintf(`
		WITH vector_results AS (
			SELECT id, content, creator_id, create_time, update_time,
				   cosine_distance(embedding, ?) as vector_distance
			FROM %s
			WHERE collection_name = ?
			  AND embedding IS NOT NULL
			ORDER BY vector_distance
			LIMIT ?
		),
		fulltext_results AS (
			SELECT id, content, creator_id, create_time, update_time,
				   MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) as relevance
			FROM %s
			WHERE collection_name = ?
			  AND MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)
			ORDER BY relevance DESC
			LIMIT ?
		),
		combined_results AS (
			SELECT 
				COALESCE(v.id, f.id) as id,
				COALESCE(v.content, f.content) as content,
				COALESCE(v.creator_id, f.creator_id) as creator_id,
				COALESCE(v.create_time, f.create_time) as create_time,
				COALESCE(v.update_time, f.update_time) as update_time,
				COALESCE(v.vector_distance, 1.0) as vector_distance,
				COALESCE(f.relevance, 0.0) as relevance,
				(0.7 * (1.0 - COALESCE(v.vector_distance, 1.0)) + 0.3 * COALESCE(f.relevance, 0.0)) as combined_score
			FROM vector_results v
			FULL OUTER JOIN fulltext_results f ON v.id = f.id
		)
		SELECT id, content, creator_id, create_time, update_time, combined_score
		FROM combined_results
		ORDER BY combined_score DESC
		LIMIT ?
	`, tableName, tableName)

	// 将向量数据转换为字符串格式
	vectorStr := fmt.Sprintf("[%s]", strings.Trim(strings.ReplaceAll(fmt.Sprintf("%v", emb[0]), " ", ","), "[]"))

	rows, err := o.db.Raw(sql, vectorStr, o.collectionName, options.TopK, query, o.collectionName, query, options.TopK, options.TopK).Rows()
	if err != nil {
		return nil, fmt.Errorf("[hybridSearch] query failed, %w", err)
	}
	defer rows.Close()

	var docs []*schema.Document
	for rows.Next() {
		var id int64
		var content string
		var creatorID int64
		var createTime, updateTime int64
		var combinedScore float64

		if err := rows.Scan(&id, &content, &creatorID, &createTime, &updateTime, &combinedScore); err != nil {
			return nil, fmt.Errorf("[hybridSearch] scan failed, %w", err)
		}

		doc := &schema.Document{
			ID:      fmt.Sprintf("%d", id),
			Content: content,
			MetaData: map[string]any{
				document.MetaDataKeyCreatorID: creatorID,
				"create_time":                 createTime,
				"update_time":                 updateTime,
			},
		}
		doc.WithScore(combinedScore)

		docs = append(docs, doc)
	}

	return docs, nil
}

func (o *oceanbaseSearchStore) vectorSearch(ctx context.Context, query string, options *retriever.Options, implSpecOptions *searchstore.RetrieverOptions) ([]*schema.Document, error) {
	// 生成查询向量
	emb, err := o.config.Embedding.EmbedStrings(ctx, []string{query})
	if err != nil {
		return nil, fmt.Errorf("[vectorSearch] embed failed, %w", err)
	}
	if len(emb) != 1 {
		return nil, fmt.Errorf("[vectorSearch] unexpected embedding size, expected=1, got=%d", len(emb))
	}

	// 构建向量搜索SQL，使用cosine_distance进行相似度计算
	sql := fmt.Sprintf(`
		SELECT id, content, creator_id, create_time, update_time,
		       cosine_distance(embedding, ?) as distance
		FROM %s
		WHERE collection_name = ?
		  AND embedding IS NOT NULL
		ORDER BY distance
		LIMIT ?
	`, o.getVectorTableName())

	// 将向量数据转换为字符串格式
	vectorStr := fmt.Sprintf("[%s]", strings.Trim(strings.ReplaceAll(fmt.Sprintf("%v", emb[0]), " ", ","), "[]"))

	rows, err := o.db.Raw(sql, vectorStr, o.collectionName, options.TopK).Rows()
	if err != nil {
		return nil, fmt.Errorf("[vectorSearch] query failed, %w", err)
	}
	defer rows.Close()

	var docs []*schema.Document
	for rows.Next() {
		var id int64
		var content string
		var creatorID int64
		var createTime, updateTime int64
		var distance float64

		if err := rows.Scan(&id, &content, &creatorID, &createTime, &updateTime, &distance); err != nil {
			return nil, fmt.Errorf("[vectorSearch] scan failed, %w", err)
		}

		doc := &schema.Document{
			ID:      fmt.Sprintf("%d", id),
			Content: content,
			MetaData: map[string]any{
				document.MetaDataKeyCreatorID: creatorID,
				"create_time":                 createTime,
				"update_time":                 updateTime,
			},
		}
		doc.WithScore(1.0 - distance) // 距离越小，分数越高

		docs = append(docs, doc)
	}

	return docs, nil
}

func (o *oceanbaseSearchStore) fulltextSearch(ctx context.Context, query string, options *retriever.Options, implSpecOptions *searchstore.RetrieverOptions) ([]*schema.Document, error) {
	// 构建全文搜索SQL，使用MATCH AGAINST进行全文检索
	sql := fmt.Sprintf(`
		SELECT id, content, creator_id, create_time, update_time,
		       MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE) as relevance
		FROM %s
		WHERE collection_name = ?
		  AND MATCH(content) AGAINST(? IN NATURAL LANGUAGE MODE)
		ORDER BY relevance DESC
		LIMIT ?
	`, o.getVectorTableName())

	rows, err := o.db.Raw(sql, query, o.collectionName, query, options.TopK).Rows()
	if err != nil {
		return nil, fmt.Errorf("[fulltextSearch] query failed, %w", err)
	}
	defer rows.Close()

	var docs []*schema.Document
	for rows.Next() {
		var id int64
		var content string
		var creatorID int64
		var createTime, updateTime int64
		var relevance float64

		if err := rows.Scan(&id, &content, &creatorID, &createTime, &updateTime, &relevance); err != nil {
			return nil, fmt.Errorf("[fulltextSearch] scan failed, %w", err)
		}

		doc := &schema.Document{
			ID:      fmt.Sprintf("%d", id),
			Content: content,
			MetaData: map[string]any{
				document.MetaDataKeyCreatorID: creatorID,
				"create_time":                 createTime,
				"update_time":                 updateTime,
			},
		}
		doc.WithScore(relevance)

		docs = append(docs, doc)
	}

	return docs, nil
}

func (o *oceanbaseSearchStore) Store(ctx context.Context, docs []*schema.Document, opts ...indexer.Option) (ids []string, err error) {
	if len(docs) == 0 {
		return nil, nil
	}

	implSpecOptions := indexer.GetImplSpecificOptions(&searchstore.IndexerOptions{}, opts...)

	defer func() {
		if err != nil {
			if implSpecOptions.ProgressBar != nil {
				_ = implSpecOptions.ProgressBar.ReportError(err)
			}
		}
	}()

	ids = make([]string, 0, len(docs))
	for _, doc := range docs {
		// 生成向量嵌入
		emb, err := o.config.Embedding.EmbedStrings(ctx, []string{doc.Content})
		if err != nil {
			return nil, fmt.Errorf("[Store] embed failed, %w", err)
		}
		if len(emb) != 1 {
			return nil, fmt.Errorf("[Store] unexpected embedding size, expected=1, got=%d", len(emb))
		}

		// 获取创建者ID
		creatorID, ok := doc.MetaData[document.MetaDataKeyCreatorID].(int64)
		if !ok {
			creatorID = 0
		}

		// 插入向量数据
		// OceanBase 向量字段需要使用特殊的语法
		vectorSQL := fmt.Sprintf(`
			INSERT INTO %s (id, collection_name, content, embedding, creator_id, create_time, update_time)
			VALUES (?, ?, ?, ?, ?, UNIX_TIMESTAMP(), UNIX_TIMESTAMP())
			ON DUPLICATE KEY UPDATE
			content = VALUES(content),
			embedding = VALUES(embedding),
			update_time = UNIX_TIMESTAMP()
		`, o.getVectorTableName())

		// 将向量数据转换为字符串格式
		vectorStr := fmt.Sprintf("[%s]", strings.Trim(strings.ReplaceAll(fmt.Sprintf("%v", emb[0]), " ", ","), "[]"))

		if err := o.db.Exec(vectorSQL, doc.ID, o.collectionName, doc.Content, vectorStr, creatorID).Error; err != nil {
			return nil, fmt.Errorf("[Store] insert vector data failed, %w", err)
		}

		ids = append(ids, doc.ID)

		if implSpecOptions.ProgressBar != nil {
			if err = implSpecOptions.ProgressBar.AddN(1); err != nil {
				return nil, err
			}
		}
	}

	return ids, nil
}

func (o *oceanbaseSearchStore) Delete(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}

	// 删除向量表中的数据
	vectorSQL := fmt.Sprintf("DELETE FROM %s WHERE id IN (?)", o.getVectorTableName())
	if err := o.db.Exec(vectorSQL, ids).Error; err != nil {
		return fmt.Errorf("[Delete] delete vector data failed, %w", err)
	}

	return nil
}

func (o *oceanbaseSearchStore) getVectorTableName() string {
	return "knowledge_vectors"
}
