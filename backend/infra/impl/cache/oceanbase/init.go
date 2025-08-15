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
	"gorm.io/gorm"

	"github.com/coze-dev/coze-studio/backend/infra/contract/cache"
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
)

// InitOceanBaseCache 初始化OceanBase缓存
func InitOceanBaseCache(db *gorm.DB) error {
	// 设置默认的Nil错误
	cache.SetDefaultNilError(cache.ErrNotFound)

	// 检查缓存表是否存在
	if err := checkAndCreateTables(db); err != nil {
		return err
	}

	// 初始化缓存客户端
	InitOceanBaseClient(db)

	logs.Info("OceanBase cache initialized successfully")
	return nil
}

// checkAndCreateTables 检查并创建缓存表
func checkAndCreateTables(db *gorm.DB) error {
	// 自动迁移表结构
	if err := db.AutoMigrate(&CacheKV{}); err != nil {
		return err
	}

	if err := db.AutoMigrate(&CacheMap{}); err != nil {
		return err
	}

	if err := db.AutoMigrate(&Message{}); err != nil {
		return err
	}

	if err := db.AutoMigrate(&MessageSubscribe{}); err != nil {
		return err
	}

	return nil
}

// GetCacheClient 获取缓存客户端
func GetCacheClient() cache.Client {
	return cache.GetClient()
}

// GetCacheCmdable 获取缓存Cmdable接口
func GetCacheCmdable() cache.Cmdable {
	client := cache.GetClient()
	return NewCmdableAdapter(client)
}
