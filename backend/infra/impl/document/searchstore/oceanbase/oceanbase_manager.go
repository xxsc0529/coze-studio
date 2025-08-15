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

	"gorm.io/gorm"

	"github.com/coze-dev/coze-studio/backend/infra/contract/document/searchstore"
	"github.com/coze-dev/coze-studio/backend/infra/contract/embedding"
)

type ManagerConfig struct {
	DB        *gorm.DB           // required
	Embedding embedding.Embedder // required
}

func NewManager(config *ManagerConfig) (searchstore.Manager, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("[NewManager] oceanbase db not provided")
	}
	if config.Embedding == nil {
		return nil, fmt.Errorf("[NewManager] oceanbase embedder not provided")
	}

	return &oceanbaseManager{config: config}, nil
}

type oceanbaseManager struct {
	config *ManagerConfig
}

func (o *oceanbaseManager) Create(ctx context.Context, req *searchstore.CreateRequest) error {
	// OceanBase的表结构已经在Docker启动时创建，这里只需要验证表是否存在

	// 检查向量表是否存在
	var count int64
	if err := o.config.DB.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", "knowledge_vectors").Count(&count).Error; err != nil {
		return fmt.Errorf("[Create] check vector table failed, %w", err)
	}
	if count == 0 {
		return fmt.Errorf("[Create] vector table knowledge_vectors not found")
	}

	// 检查全文索引表是否存在
	if err := o.config.DB.Raw("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", "project_search").Count(&count).Error; err != nil {
		return fmt.Errorf("[Create] check fulltext table failed, %w", err)
	}
	if count == 0 {
		return fmt.Errorf("[Create] fulltext table project_search not found")
	}

	return nil
}

func (o *oceanbaseManager) Drop(ctx context.Context, req *searchstore.DropRequest) error {
	// 删除向量表中的数据
	if err := o.config.DB.Exec("DELETE FROM knowledge_vectors WHERE collection_name = ?", req.CollectionName).Error; err != nil {
		return fmt.Errorf("[Drop] delete vector data failed, %w", err)
	}

	// 删除全文索引表中的数据
	if err := o.config.DB.Exec("DELETE FROM project_search WHERE id IN (SELECT id FROM knowledge_vectors WHERE collection_name = ?)", req.CollectionName).Error; err != nil {
		return fmt.Errorf("[Drop] delete fulltext data failed, %w", err)
	}

	return nil
}

func (o *oceanbaseManager) GetType() searchstore.SearchStoreType {
	return searchstore.TypeVectorStore // 支持向量搜索，同时集成全文搜索功能
}

func (o *oceanbaseManager) GetSearchStore(ctx context.Context, collectionName string) (searchstore.SearchStore, error) {
	return &oceanbaseSearchStore{
		config:         o.config,
		collectionName: collectionName,
		db:             o.config.DB,
	}, nil
}

func (o *oceanbaseManager) GetEmbedding() embedding.Embedder {
	return o.config.Embedding
}
