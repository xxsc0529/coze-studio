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

package knowledge

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/cloudwego/eino-ext/components/embedding/ark"
	ollamaEmb "github.com/cloudwego/eino-ext/components/embedding/ollama"
	"github.com/cloudwego/eino-ext/components/embedding/openai"
	"github.com/cloudwego/eino/components/prompt"
	"github.com/cloudwego/eino/schema"
	"github.com/milvus-io/milvus/client/v2/milvusclient"
	"github.com/volcengine/volc-sdk-golang/service/vikingdb"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	"github.com/coze-dev/coze-studio/backend/application/internal"
	"github.com/coze-dev/coze-studio/backend/application/search"
	knowledgeImpl "github.com/coze-dev/coze-studio/backend/domain/knowledge/service"
	"github.com/coze-dev/coze-studio/backend/infra/contract/cache"
	"github.com/coze-dev/coze-studio/backend/infra/contract/document/nl2sql"
	"github.com/coze-dev/coze-studio/backend/infra/contract/document/ocr"
	"github.com/coze-dev/coze-studio/backend/infra/contract/document/parser"
	"github.com/coze-dev/coze-studio/backend/infra/contract/document/searchstore"
	"github.com/coze-dev/coze-studio/backend/infra/contract/embedding"
	"github.com/coze-dev/coze-studio/backend/infra/contract/es"
	"github.com/coze-dev/coze-studio/backend/infra/contract/idgen"
	"github.com/coze-dev/coze-studio/backend/infra/contract/imagex"
	"github.com/coze-dev/coze-studio/backend/infra/contract/messages2query"
	"github.com/coze-dev/coze-studio/backend/infra/contract/rdb"
	"github.com/coze-dev/coze-studio/backend/infra/contract/storage"
	chatmodelImpl "github.com/coze-dev/coze-studio/backend/infra/impl/chatmodel"
	builtinNL2SQL "github.com/coze-dev/coze-studio/backend/infra/impl/document/nl2sql/builtin"
	"github.com/coze-dev/coze-studio/backend/infra/impl/document/rerank/rrf"
	sses "github.com/coze-dev/coze-studio/backend/infra/impl/document/searchstore/elasticsearch"
	ssmilvus "github.com/coze-dev/coze-studio/backend/infra/impl/document/searchstore/milvus"
	ssoceanbase "github.com/coze-dev/coze-studio/backend/infra/impl/document/searchstore/oceanbase"
	ssvikingdb "github.com/coze-dev/coze-studio/backend/infra/impl/document/searchstore/vikingdb"
	arkemb "github.com/coze-dev/coze-studio/backend/infra/impl/embedding/ark"
	"github.com/coze-dev/coze-studio/backend/infra/impl/embedding/http"
	"github.com/coze-dev/coze-studio/backend/infra/impl/embedding/wrap"
	"github.com/coze-dev/coze-studio/backend/infra/impl/eventbus"
	builtinM2Q "github.com/coze-dev/coze-studio/backend/infra/impl/messages2query/builtin"
	"github.com/coze-dev/coze-studio/backend/pkg/lang/conv"
	"github.com/coze-dev/coze-studio/backend/pkg/lang/ptr"
	"github.com/coze-dev/coze-studio/backend/pkg/logs"
	"github.com/coze-dev/coze-studio/backend/types/consts"
)

type ServiceComponents struct {
	DB            *gorm.DB
	IDGenSVC      idgen.IDGenerator
	Storage       storage.Storage
	RDB           rdb.RDB
	ImageX        imagex.ImageX
	ES            es.Client
	EventBus      search.ResourceEventBus
	CacheCli      cache.Cmdable
	OCR           ocr.OCR
	ParserManager parser.Manager
}

func InitService(c *ServiceComponents) (*KnowledgeApplicationService, error) {
	ctx := context.Background()

	nameServer := os.Getenv(consts.MQServer)

	knowledgeProducer, err := eventbus.NewProducer(nameServer, consts.RMQTopicKnowledge, consts.RMQConsumeGroupKnowledge, 2)
	if err != nil {
		return nil, fmt.Errorf("init knowledge producer failed, err=%w", err)
	}

	var sManagers []searchstore.Manager

	// 根据配置选择全文搜索后端
	searchBackend := os.Getenv("SEARCH_BACKEND")
	if searchBackend == "" {
		searchBackend = "oceanbase" // 默认使用OceanBase
	}

	if searchBackend == "oceanbase" {
		// 使用OceanBase作为全文搜索后端
		logs.Infof("Using OceanBase as full-text search backend")
		// OceanBase的全文搜索功能已经集成在向量搜索中，不需要单独添加
	} else {
		// 使用Elasticsearch作为全文搜索后端
		logs.Infof("Using Elasticsearch as full-text search backend")
		sManagers = append(sManagers, sses.NewManager(&sses.ManagerConfig{Client: c.ES}))
	}

	// vector search
	mgr, err := getVectorStore(ctx)
	if err != nil {
		return nil, fmt.Errorf("init vector store failed, err=%w", err)
	}
	sManagers = append(sManagers, mgr)

	root, err := os.Getwd()
	if err != nil {
		logs.Warnf("[InitConfig] Failed to get current working directory: %v", err)
		root = os.Getenv("PWD")
	}

	var rewriter messages2query.MessagesToQuery
	if rewriterChatModel, _, err := internal.GetBuiltinChatModel(ctx, "M2Q_"); err != nil {
		return nil, err
	} else {
		filePath := filepath.Join(root, "resources/conf/prompt/messages_to_query_template_jinja2.json")
		rewriterTemplate, err := readJinja2PromptTemplate(filePath)
		if err != nil {
			return nil, err
		}
		rewriter, err = builtinM2Q.NewMessagesToQuery(ctx, rewriterChatModel, rewriterTemplate)
		if err != nil {
			return nil, err
		}
	}

	var n2s nl2sql.NL2SQL
	if n2sChatModel, _, err := internal.GetBuiltinChatModel(ctx, "NL2SQL_"); err != nil {
		return nil, err
	} else {
		filePath := filepath.Join(root, "resources/conf/prompt/nl2sql_template_jinja2.json")
		n2sTemplate, err := readJinja2PromptTemplate(filePath)
		if err != nil {
			return nil, err
		}
		n2s, err = builtinNL2SQL.NewNL2SQL(ctx, n2sChatModel, n2sTemplate)
		if err != nil {
			return nil, err
		}
	}

	knowledgeDomainSVC, knowledgeEventHandler := knowledgeImpl.NewKnowledgeSVC(&knowledgeImpl.KnowledgeSVCConfig{
		DB:                  c.DB,
		IDGen:               c.IDGenSVC,
		RDB:                 c.RDB,
		Producer:            knowledgeProducer,
		SearchStoreManagers: sManagers,
		ParseManager:        c.ParserManager,
		Storage:             c.Storage,
		Rewriter:            rewriter,
		Reranker:            rrf.NewRRFReranker(0), // default rrf
		NL2Sql:              n2s,
		OCR:                 c.OCR,
		CacheCli:            c.CacheCli,
		ModelFactory:        chatmodelImpl.NewDefaultFactory(),
	})

	if err = eventbus.DefaultSVC().RegisterConsumer(nameServer, consts.RMQTopicKnowledge, consts.RMQConsumeGroupKnowledge, knowledgeEventHandler); err != nil {
		return nil, fmt.Errorf("register knowledge consumer failed, err=%w", err)
	}

	KnowledgeSVC.DomainSVC = knowledgeDomainSVC
	KnowledgeSVC.eventBus = c.EventBus
	KnowledgeSVC.storage = c.Storage
	return KnowledgeSVC, nil
}

func getVectorStore(ctx context.Context) (searchstore.Manager, error) {
	vsType := os.Getenv("VECTOR_STORE_TYPE")

	switch vsType {
	case "oceanbase":
		// 使用OceanBase作为向量存储
		db, err := getOceanBaseDB()
		if err != nil {
			return nil, fmt.Errorf("init oceanbase db failed, err=%w", err)
		}

		emb, err := getEmbedding(ctx)
		if err != nil {
			return nil, fmt.Errorf("init oceanbase embedding failed, err=%w", err)
		}

		mgr, err := ssoceanbase.NewManager(&ssoceanbase.ManagerConfig{
			DB:        db,
			Embedding: emb,
		})
		if err != nil {
			return nil, fmt.Errorf("init oceanbase vector store failed, err=%w", err)
		}

		return mgr, nil
	case "milvus":
		cctx, cancel := context.WithTimeout(ctx, time.Second*5)
		defer cancel()

		milvusAddr := os.Getenv("MILVUS_ADDR")
		mc, err := milvusclient.New(cctx, &milvusclient.ClientConfig{Address: milvusAddr})
		if err != nil {
			return nil, fmt.Errorf("init milvus client failed, err=%w", err)
		}

		emb, err := getEmbedding(ctx)
		if err != nil {
			return nil, fmt.Errorf("init milvus embedding failed, err=%w", err)
		}

		mgr, err := ssmilvus.NewManager(&ssmilvus.ManagerConfig{
			Client:       mc,
			Embedding:    emb,
			EnableHybrid: ptr.Of(true),
		})
		if err != nil {
			return nil, fmt.Errorf("init milvus vector store failed, err=%w", err)
		}

		return mgr, nil
	case "vikingdb":
		var (
			host      = os.Getenv("VIKING_DB_HOST")
			region    = os.Getenv("VIKING_DB_REGION")
			ak        = os.Getenv("VIKING_DB_AK")
			sk        = os.Getenv("VIKING_DB_SK")
			scheme    = os.Getenv("VIKING_DB_SCHEME")
			modelName = os.Getenv("VIKING_DB_MODEL_NAME")
		)
		if ak == "" || sk == "" {
			return nil, fmt.Errorf("invalid vikingdb ak / sk")
		}
		if host == "" {
			host = "api-vikingdb.volces.com"
		}
		if region == "" {
			region = "cn-beijing"
		}
		if scheme == "" {
			scheme = "https"
		}

		var embConfig *ssvikingdb.VikingEmbeddingConfig
		if modelName != "" {
			embName := ssvikingdb.VikingEmbeddingModelName(modelName)
			if embName.Dimensions() == 0 {
				return nil, fmt.Errorf("embedding model not support, model_name=%s", modelName)
			}
			embConfig = &ssvikingdb.VikingEmbeddingConfig{
				UseVikingEmbedding: true,
				EnableHybrid:       embName.SupportStatus() == embedding.SupportDenseAndSparse,
				ModelName:          embName,
				ModelVersion:       embName.ModelVersion(),
				DenseWeight:        ptr.Of(0.2),
				BuiltinEmbedding:   nil,
			}
		} else {
			builtinEmbedding, err := getEmbedding(ctx)
			if err != nil {
				return nil, fmt.Errorf("builtint embedding init failed, err=%w", err)
			}

			embConfig = &ssvikingdb.VikingEmbeddingConfig{
				UseVikingEmbedding: false,
				EnableHybrid:       false,
				BuiltinEmbedding:   builtinEmbedding,
			}
		}
		svc := vikingdb.NewVikingDBService(host, region, ak, sk, scheme)
		mgr, err := ssvikingdb.NewManager(&ssvikingdb.ManagerConfig{
			Service:         svc,
			IndexingConfig:  nil, // use default config
			EmbeddingConfig: embConfig,
		})
		if err != nil {
			return nil, fmt.Errorf("init vikingdb manager failed, err=%w", err)
		}

		return mgr, nil

	default:
		return nil, fmt.Errorf("unexpected vector store type, type=%s", vsType)
	}
}

func getOceanBaseDB() (*gorm.DB, error) {
	// 从环境变量获取OceanBase连接信息
	dsn := os.Getenv("OCEANBASE_DSN")
	if dsn == "" {
		// 默认连接字符串
		dsn = "root@test:coze123@tcp(localhost:2881)/opencoze?charset=utf8mb4&parseTime=True&loc=Local"
	}

	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("connect to oceanbase failed: %w", err)
	}

	return db, nil
}

func getEmbedding(ctx context.Context) (embedding.Embedder, error) {
	var batchSize int
	if bs, err := strconv.ParseInt(os.Getenv("EMBEDDING_MAX_BATCH_SIZE"), 10, 64); err != nil {
		logs.CtxWarnf(ctx, "EMBEDDING_MAX_BATCH_SIZE not set / invalid, using default batchSize=100")
		batchSize = 100
	} else {
		batchSize = int(bs)
	}

	var emb embedding.Embedder

	switch os.Getenv("EMBEDDING_TYPE") {
	case "openai":
		var (
			openAIEmbeddingBaseURL     = os.Getenv("OPENAI_EMBEDDING_BASE_URL")
			openAIEmbeddingModel       = os.Getenv("OPENAI_EMBEDDING_MODEL")
			openAIEmbeddingApiKey      = os.Getenv("OPENAI_EMBEDDING_API_KEY")
			openAIEmbeddingByAzure     = os.Getenv("OPENAI_EMBEDDING_BY_AZURE")
			openAIEmbeddingApiVersion  = os.Getenv("OPENAI_EMBEDDING_API_VERSION")
			openAIEmbeddingDims        = os.Getenv("OPENAI_EMBEDDING_DIMS")
			openAIRequestEmbeddingDims = os.Getenv("OPENAI_EMBEDDING_REQUEST_DIMS")
		)

		byAzure, err := strconv.ParseBool(openAIEmbeddingByAzure)
		if err != nil {
			return nil, fmt.Errorf("init openai embedding by_azure failed, err=%w", err)
		}

		dims, err := strconv.ParseInt(openAIEmbeddingDims, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("init openai embedding dims failed, err=%w", err)
		}

		openAICfg := &openai.EmbeddingConfig{
			APIKey:     openAIEmbeddingApiKey,
			ByAzure:    byAzure,
			BaseURL:    openAIEmbeddingBaseURL,
			APIVersion: openAIEmbeddingApiVersion,
			Model:      openAIEmbeddingModel,
			// Dimensions: ptr.Of(int(dims)),
		}
		reqDims := conv.StrToInt64D(openAIRequestEmbeddingDims, 0)
		if reqDims > 0 {
			// some openai model not support request dims
			openAICfg.Dimensions = ptr.Of(int(reqDims))
		}

		emb, err = wrap.NewOpenAIEmbedder(ctx, openAICfg, dims, batchSize)
		if err != nil {
			return nil, fmt.Errorf("init openai embedding failed, err=%w", err)
		}

	case "ark":
		var (
			arkEmbeddingBaseURL = os.Getenv("ARK_EMBEDDING_BASE_URL")
			arkEmbeddingModel   = os.Getenv("ARK_EMBEDDING_MODEL")
			arkEmbeddingApiKey  = os.Getenv("ARK_EMBEDDING_API_KEY")
			// deprecated: use ARK_EMBEDDING_API_KEY instead
			// ARK_EMBEDDING_AK will be removed in the future
			arkEmbeddingAK      = os.Getenv("ARK_EMBEDDING_AK")
			arkEmbeddingDims    = os.Getenv("ARK_EMBEDDING_DIMS")
			arkEmbeddingAPIType = os.Getenv("ARK_EMBEDDING_API_TYPE")
		)

		dims, err := strconv.ParseInt(arkEmbeddingDims, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("init ark embedding dims failed, err=%w", err)
		}

		apiType := ark.APITypeText
		if arkEmbeddingAPIType != "" {
			if t := ark.APIType(arkEmbeddingAPIType); t != ark.APITypeText && t != ark.APITypeMultiModal {
				return nil, fmt.Errorf("init ark embedding api_type failed, invalid api_type=%s", t)
			} else {
				apiType = t
			}
		}

		emb, err = arkemb.NewArkEmbedder(ctx, &ark.EmbeddingConfig{
			APIKey: func() string {
				if arkEmbeddingApiKey != "" {
					return arkEmbeddingApiKey
				}
				return arkEmbeddingAK
			}(),
			Model:   arkEmbeddingModel,
			BaseURL: arkEmbeddingBaseURL,
			APIType: &apiType,
		}, dims, batchSize)
		if err != nil {
			return nil, fmt.Errorf("init ark embedding client failed, err=%w", err)
		}

	case "ollama":
		var (
			ollamaEmbeddingBaseURL = os.Getenv("OLLAMA_EMBEDDING_BASE_URL")
			ollamaEmbeddingModel   = os.Getenv("OLLAMA_EMBEDDING_MODEL")
			ollamaEmbeddingDims    = os.Getenv("OLLAMA_EMBEDDING_DIMS")
		)

		dims, err := strconv.ParseInt(ollamaEmbeddingDims, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("init ollama embedding dims failed, err=%w", err)
		}

		emb, err = wrap.NewOllamaEmbedder(ctx, &ollamaEmb.EmbeddingConfig{
			BaseURL: ollamaEmbeddingBaseURL,
			Model:   ollamaEmbeddingModel,
		}, dims, batchSize)
		if err != nil {
			return nil, fmt.Errorf("init ollama embedding failed, err=%w", err)
		}

	case "http":
		var (
			httpEmbeddingBaseURL = os.Getenv("HTTP_EMBEDDING_ADDR")
			httpEmbeddingDims    = os.Getenv("HTTP_EMBEDDING_DIMS")
		)
		dims, err := strconv.ParseInt(httpEmbeddingDims, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("init http embedding dims failed, err=%w", err)
		}
		emb, err = http.NewEmbedding(httpEmbeddingBaseURL, dims, batchSize)
		if err != nil {
			return nil, fmt.Errorf("init http embedding failed, err=%w", err)
		}

	default:
		return nil, fmt.Errorf("init knowledge embedding failed, type not configured")
	}

	return emb, nil
}

func readJinja2PromptTemplate(jsonFilePath string) (prompt.ChatTemplate, error) {
	b, err := os.ReadFile(jsonFilePath)
	if err != nil {
		return nil, err
	}
	var m2qMessages []*schema.Message
	if err = json.Unmarshal(b, &m2qMessages); err != nil {
		return nil, err
	}
	tpl := make([]schema.MessagesTemplate, len(m2qMessages))
	for i := range m2qMessages {
		tpl[i] = m2qMessages[i]
	}
	return prompt.FromMessages(schema.Jinja2, tpl...), nil
}
