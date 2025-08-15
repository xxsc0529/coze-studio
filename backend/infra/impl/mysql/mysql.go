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

package mysql

import (
	"fmt"
	"os"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func New() (*gorm.DB, error) {
	// 优先使用OceanBase连接字符串，如果没有则使用MySQL
	dsn := os.Getenv("OCEANBASE_DSN")
	if dsn == "" {
		dsn = os.Getenv("MYSQL_DSN")
	}

	// 如果都没有设置，使用默认的OceanBase连接
	if dsn == "" {
		dsn = "root@test:coze123@tcp(localhost:2881)/opencoze?charset=utf8mb4&parseTime=True&loc=Local"
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("database open failed, dsn: %s, err: %w", dsn, err)
	}

	return db, nil
}
