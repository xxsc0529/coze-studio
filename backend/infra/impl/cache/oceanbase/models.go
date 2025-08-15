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

import "time"

// CacheKV 缓存键值对表
type CacheKV struct {
	ID         int64     `json:"id" gorm:"column:id;primaryKey;type:bigint(20) auto_increment"`
	CacheKey   string    `json:"cache_key" gorm:"column:cache_key;type:varchar(256);not null;unique"`
	CacheValue []byte    `json:"cache_value" gorm:"column:cache_value;type:longblob;not null"`
	ExpireTime time.Time `json:"expire_time" gorm:"index"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// TableName 指定表名
func (CacheKV) TableName() string {
	return "cache_kvs"
}

// CacheMap 缓存映射表
type CacheMap struct {
	ID         int64     `json:"id" gorm:"column:id;primaryKey;type:bigint(20) auto_increment"`
	CacheKey   string    `json:"cache_key" gorm:"column:cache_key;type:varchar(256);not null;uniqueIndex:idx_cache_key_field"`
	CacheField string    `json:"cache_field" gorm:"column:cache_field;type:varchar(256);not null;uniqueIndex:idx_cache_key_field"`
	CacheValue string    `json:"cache_value" gorm:"column:cache_value;type:longblob;not null"`
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// TableName 指定表名
func (CacheMap) TableName() string {
	return "cache_maps"
}

// Message 消息表
type Message struct {
	ID        int64     `json:"id" gorm:"column:id;primaryKey;type:bigint(20) auto_increment"`
	Channel   string    `json:"channel" gorm:"column:channel;type:varchar(1024);not null;index"`
	Message   string    `json:"message" gorm:"column:message;type:text;not null"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// TableName 指定表名
func (Message) TableName() string {
	return "cache_messages"
}

// MessageSubscribe 消息订阅表
type MessageSubscribe struct {
	Channel       string    `json:"channel" gorm:"column:channel;type:varchar(1024);not null;uniqueIndex:idx_channel_subscriber"`
	Subscriber    string    `json:"subscriber" gorm:"column:subscriber;type:varchar(1024);not null;uniqueIndex:idx_channel_subscriber"`
	LastMessageId int64     `json:"last_message_id" gorm:"column:last_message_id;type:bigint(20);not null;default:-1"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// TableName 指定表名
func (MessageSubscribe) TableName() string {
	return "cache_message_subscribes"
}
