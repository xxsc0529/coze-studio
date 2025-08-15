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
	"strconv"
	"time"

	"github.com/coze-dev/coze-studio/backend/infra/contract/cache"
)

// CmdableAdapter 将cache.Client适配为cache.Cmdable接口
type CmdableAdapter struct {
	client cache.Client
}

// NewCmdableAdapter 创建新的适配器
func NewCmdableAdapter(client cache.Client) cache.Cmdable {
	return &CmdableAdapter{client: client}
}

// Pipeline 实现cache.Cmdable接口
func (c *CmdableAdapter) Pipeline() cache.Pipeliner {
	// OceanBase不支持管道操作，返回一个空实现
	return &PipelinerAdapter{}
}

// Set 实现StringCmdable接口
func (c *CmdableAdapter) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) cache.StatusCmd {
	err := c.client.Set(key, value, expiration)
	return &StatusCmdAdapter{err: err}
}

// Get 实现StringCmdable接口
func (c *CmdableAdapter) Get(ctx context.Context, key string) cache.StringCmd {
	val, err := c.client.GetString(key)
	return &StringCmdAdapter{val: val, err: err}
}

// IncrBy 实现StringCmdable接口
func (c *CmdableAdapter) IncrBy(ctx context.Context, key string, value int64) cache.IntCmd {
	// 使用数据库事务实现原子递增
	var result int64
	err := c.client.Transaction(func(context cache.Context) error {
		// 获取当前值
		currentVal, err := context.Get().GetString(key)
		if err != nil && err != cache.ErrNotFound {
			return err
		}

		var current int64
		if err == cache.ErrNotFound {
			current = 0
		} else {
			current, err = strconv.ParseInt(currentVal, 10, 64)
			if err != nil {
				return err
			}
		}

		// 计算新值
		result = current + value

		// 设置新值
		return context.Get().Set(key, strconv.FormatInt(result, 10), 0)
	})

	if err != nil {
		return &IntCmdAdapter{err: err}
	}

	return &IntCmdAdapter{val: result}
}

// Incr 实现StringCmdable接口
func (c *CmdableAdapter) Incr(ctx context.Context, key string) cache.IntCmd {
	// 使用IncrBy实现Incr
	return c.IncrBy(ctx, key, 1)
}

// HSet 实现HashCmdable接口
func (c *CmdableAdapter) HSet(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	// 将values转换为map字段
	if len(values) >= 2 {
		for i := 0; i < len(values)-1; i += 2 {
			field, ok1 := values[i].(string)
			value, ok2 := values[i+1].(string)
			if ok1 && ok2 {
				err := c.client.SetMapField(key, field, value)
				if err != nil {
					return &IntCmdAdapter{err: err}
				}
			}
		}
		return &IntCmdAdapter{val: 1}
	}
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

// HGetAll 实现HashCmdable接口
func (c *CmdableAdapter) HGetAll(ctx context.Context, key string) cache.MapStringStringCmd {
	val, err := c.client.GetMap(key)
	return &MapStringStringCmdAdapter{val: val, err: err}
}

// Del 实现GenericCmdable接口
func (c *CmdableAdapter) Del(ctx context.Context, keys ...string) cache.IntCmd {
	if len(keys) == 0 {
		return &IntCmdAdapter{val: 0}
	}

	var totalDeleted int64
	for _, key := range keys {
		deleted, err := c.client.Delete(key)
		if err != nil {
			return &IntCmdAdapter{err: err}
		}
		totalDeleted += deleted
	}

	return &IntCmdAdapter{val: totalDeleted}
}

// Exists 实现GenericCmdable接口
func (c *CmdableAdapter) Exists(ctx context.Context, keys ...string) cache.IntCmd {
	if len(keys) == 0 {
		return &IntCmdAdapter{val: 0}
	}

	count, err := c.client.Count(keys...)
	return &IntCmdAdapter{val: count, err: err}
}

// Expire 实现GenericCmdable接口
func (c *CmdableAdapter) Expire(ctx context.Context, key string, expiration time.Duration) cache.BoolCmd {
	success, err := c.client.Expire(key, expiration)
	return &BoolCmdAdapter{val: success, err: err}
}

// LIndex 实现ListCmdable接口
func (c *CmdableAdapter) LIndex(ctx context.Context, key string, index int64) cache.StringCmd {
	// OceanBase不支持列表操作，返回错误
	return &StringCmdAdapter{err: cache.ErrNotFound}
}

// LPush 实现ListCmdable接口
func (c *CmdableAdapter) LPush(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	// OceanBase不支持列表操作，返回错误
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

// RPush 实现ListCmdable接口
func (c *CmdableAdapter) RPush(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	// OceanBase不支持列表操作，返回错误
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

// LSet 实现ListCmdable接口
func (c *CmdableAdapter) LSet(ctx context.Context, key string, index int64, value interface{}) cache.StatusCmd {
	// OceanBase不支持列表操作，返回错误
	return &StatusCmdAdapter{err: cache.ErrNotFound}
}

// LPop 实现ListCmdable接口
func (c *CmdableAdapter) LPop(ctx context.Context, key string) cache.StringCmd {
	// OceanBase不支持列表操作，返回错误
	return &StringCmdAdapter{err: cache.ErrNotFound}
}

// LRange 实现ListCmdable接口
func (c *CmdableAdapter) LRange(ctx context.Context, key string, start, stop int64) cache.StringSliceCmd {
	// OceanBase不支持列表操作，返回错误
	return &StringSliceCmdAdapter{err: cache.ErrNotFound}
}

// 适配器实现各种Cmd接口
type StatusCmdAdapter struct {
	err error
}

func (s *StatusCmdAdapter) Err() error {
	return s.err
}

func (s *StatusCmdAdapter) Result() (string, error) {
	return "", s.err
}

type StringCmdAdapter struct {
	val string
	err error
}

func (s *StringCmdAdapter) Err() error {
	return s.err
}

func (s *StringCmdAdapter) Result() (string, error) {
	return s.val, s.err
}

func (s *StringCmdAdapter) Val() string {
	return s.val
}

func (s *StringCmdAdapter) Int64() (int64, error) {
	return 0, s.err
}

func (s *StringCmdAdapter) Bytes() ([]byte, error) {
	return []byte(s.val), s.err
}

type IntCmdAdapter struct {
	val int64
	err error
}

func (i *IntCmdAdapter) Err() error {
	return i.err
}

func (i *IntCmdAdapter) Result() (int64, error) {
	return i.val, i.err
}

type BoolCmdAdapter struct {
	val bool
	err error
}

func (b *BoolCmdAdapter) Err() error {
	return b.err
}

func (b *BoolCmdAdapter) Result() (bool, error) {
	return b.val, b.err
}

type MapStringStringCmdAdapter struct {
	val map[string]string
	err error
}

func (m *MapStringStringCmdAdapter) Err() error {
	return m.err
}

func (m *MapStringStringCmdAdapter) Result() (map[string]string, error) {
	return m.val, m.err
}

type StringSliceCmdAdapter struct {
	val []string
	err error
}

func (s *StringSliceCmdAdapter) Err() error {
	return s.err
}

func (s *StringSliceCmdAdapter) Result() ([]string, error) {
	return s.val, s.err
}

type PipelinerAdapter struct{}

func (p *PipelinerAdapter) Pipeline() cache.Pipeliner {
	return p
}

func (p *PipelinerAdapter) Exec(ctx context.Context) ([]cache.Cmder, error) {
	return nil, cache.ErrNotFound
}

func (p *PipelinerAdapter) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) cache.StatusCmd {
	return &StatusCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) Get(ctx context.Context, key string) cache.StringCmd {
	return &StringCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) IncrBy(ctx context.Context, key string, value int64) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) Incr(ctx context.Context, key string) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) HSet(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) HGetAll(ctx context.Context, key string) cache.MapStringStringCmd {
	return &MapStringStringCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) Del(ctx context.Context, keys ...string) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) Exists(ctx context.Context, keys ...string) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) Expire(ctx context.Context, key string, expiration time.Duration) cache.BoolCmd {
	return &BoolCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) LIndex(ctx context.Context, key string, index int64) cache.StringCmd {
	return &StringCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) LPush(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) RPush(ctx context.Context, key string, values ...interface{}) cache.IntCmd {
	return &IntCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) LSet(ctx context.Context, key string, index int64, value interface{}) cache.StatusCmd {
	return &StatusCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) LPop(ctx context.Context, key string) cache.StringCmd {
	return &StringCmdAdapter{err: cache.ErrNotFound}
}

func (p *PipelinerAdapter) LRange(ctx context.Context, key string, start, stop int64) cache.StringSliceCmd {
	return &StringSliceCmdAdapter{err: cache.ErrNotFound}
}
