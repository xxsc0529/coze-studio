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

package cache

import (
	"errors"
	"time"
)

// Context 缓存上下文接口
type Context interface {
	Get() Client
}

// Client 缓存客户端接口
type Client interface {
	Close() error
	Set(key string, value any, expire time.Duration) error
	GetBytes(key string) ([]byte, error)
	GetString(key string) (string, error)
	Delete(key string) (int64, error)
	Count(key ...string) (int64, error)
	SetMapField(key string, field string, value string) error
	GetMapField(key string, field string) (string, error)
	DeleteMapField(key string, field string) error
	GetMap(key string) (map[string]string, error)
	ScanMapStream(key string, cursor uint64, match string, count int64) ([]string, uint64, error)
	SetNX(key string, value any, expire time.Duration) (bool, error)
	Expire(key string, expire time.Duration) (bool, error)
	Transaction(fn func(context Context) error) error
	Publish(channel string, message string) error
	Subscribe(channel string) (<-chan string, func())
}

var (
	client Client

	ErrNotInit  = errors.New("cache not init")
	ErrNotFound = errors.New("cache not found")
)

// SetClient 设置缓存客户端
func SetClient(c Client) {
	client = c
}

// GetClient 获取缓存客户端
func GetClient() Client {
	return client
}

// Close 关闭缓存客户端
func Close() error {
	if client == nil {
		return ErrNotInit
	}
	return client.Close()
}
