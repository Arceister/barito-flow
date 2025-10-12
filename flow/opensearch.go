package flow

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/BaritoLog/barito-flow/prome"
	pb "github.com/bentol/barito-proto/producer"
	"github.com/golang/protobuf/jsonpb"
	opensearch "github.com/opensearch-project/opensearch-go/v4"
	opensearchapi "github.com/opensearch-project/opensearch-go/v4/opensearchapi"
	log "github.com/sirupsen/logrus"
	"github.com/zekroTJA/timedmap"
)

var _ Elastic = (*openSearchClient)(nil)
var openSearchCounter = 0

type openSearchClient struct {
	client           *opensearchapi.Client
	onFailureFunc    func(*pb.Timber)
	onStoreFunc      func(ctx context.Context, indexName, document string) (err error)
	jsonMarshaler    *jsonpb.Marshaler
	indexExistsCache *timedmap.TimedMap

	numOfShards                        int
	numOfReplicas                      int
	dataStreamDefaultComponentTemplate string

	redactor Redactor
}

type openSearchConfig struct {
	numOfShards                        int
	numOfReplicas                      int
	dataStreamDefaultComponentTemplate string
}

func NewOpenSearchConfig(
	numOfShards int,
	numOfReplicas int,
	dataStreamDefaultComponentTemplate string,
) openSearchConfig {
	return openSearchConfig{
		numOfShards:                        numOfShards,
		numOfReplicas:                      numOfReplicas,
		dataStreamDefaultComponentTemplate: dataStreamDefaultComponentTemplate,
	}
}

func NewOpenSearch(config openSearchConfig, urls []string, openSearchUsername string, openSearchPassword string, httpClient *http.Client) (*openSearchClient, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	var openSearchConfig opensearch.Config
	if openSearchUsername == "" || openSearchPassword == "" {
		openSearchConfig = opensearch.Config{
			Addresses:            urls,
			Transport:            httpClient.Transport,
			MaxRetries:           0,
			EnableRetryOnTimeout: true,
		}
	} else {
		openSearchConfig = opensearch.Config{
			Addresses:            urls,
			Username:             openSearchUsername,
			Password:             openSearchPassword,
			MaxRetries:           0,
			EnableRetryOnTimeout: true,
		}
	}

	c, err := opensearchapi.NewClient(
		opensearchapi.Config{
			Client: openSearchConfig,
		},
	)
	if err != nil {
		return nil, err
	}

	client := &openSearchClient{
		client:           c,
		jsonMarshaler:    &jsonpb.Marshaler{},
		indexExistsCache: timedmap.New(10 * time.Minute),
		redactor:         &DummyRedactor{},

		numOfShards:                        config.numOfShards,
		numOfReplicas:                      config.numOfReplicas,
		dataStreamDefaultComponentTemplate: config.dataStreamDefaultComponentTemplate,
	}
	client.onStoreFunc = client.bulkInsertDataStream

	return client, nil
}

func (o *openSearchClient) Store(ctx context.Context, timber pb.Timber) (err error) {
	indexPrefix := timber.GetContext().GetEsIndexPrefix()
	indexName := indexPrefix
	appSecret := timber.GetContext().GetAppSecret()

	for {
		if o.ensureIndexExists(ctx, indexName) {
			break
		}
		prome.IncreaseConsumerFailedToEnsureIndexExists(indexName)
		time.Sleep(5 * time.Second)
	}

	document, err := ConvertTimberToEsDocumentString(timber, o.jsonMarshaler)
	if err != nil {
		prome.IncreaseConsumerTimberConvertError(indexPrefix)
		return err
	}

	redactDocument, err := o.redactor.Redact(indexPrefix, document)
	if err != nil {
		log.Error("Error redacting document: ", err)
		return err
	}

	err = o.onStoreFunc(ctx, indexName, redactDocument)
	openSearchCounter++
	instruOSStore(appSecret, err)

	return nil
}

func (o *openSearchClient) ensureIndexExists(ctx context.Context, indexName string) bool {
	indexCacheFound := o.indexExistsCache.GetValue(indexName)
	if indexCacheFound != nil {
		return true
	}

	exists, err := o.isIndexExists(ctx, indexName)
	if err != nil {
		log.Errorf("Error checking if index exists: %s", err)
		return false
	}

	if !exists {
		return o.ensureIndexDataStreamExists(ctx, indexName)
	}

	o.indexExistsCache.Set(indexName, true, 10*time.Minute)
	return true
}

func (o *openSearchClient) ensureIndexDataStreamExists(ctx context.Context, datastreamName string) bool {
	log.Warnf("OpenSearch datastream index '%s' is not exist", datastreamName)

	if o.createIndexComponentTemplate(ctx) != nil {
		return false
	}

	if o.createIndexTemplate(ctx, datastreamName) != nil {
		return false
	}

	if o.createDataStream(ctx, datastreamName) != nil {
		return false
	}

	return true
}

func (o *openSearchClient) isIndexExists(ctx context.Context, indexName string) (bool, error) {
	existsReq := opensearchapi.IndicesExistsReq{
		Indices: []string{indexName},
	}
	existsResp, err := o.client.Indices.Exists(ctx, existsReq)
	if err != nil {
		log.Errorf("Failed to check index existence for %s: %v", indexName, err)
		return false, nil
	}
	defer existsResp.Body.Close()

	if existsResp.StatusCode == 200 {
		o.indexExistsCache.Set(indexName, true, 10*time.Minute)
		return true, nil
	}

	if existsResp.StatusCode == 404 {
		return false, nil
	}

	return false, nil
}

func (o *openSearchClient) createIndexComponentTemplate(ctx context.Context) error {
	createIndexComponentTemplateReq := opensearchapi.ComponentTemplateCreateReq{
		ComponentTemplate: o.dataStreamDefaultComponentTemplate,
		Body: strings.NewReader(`{
			"template": {
				"settings": {
					"codec": "best_compression",
					"refresh_interval": "30s"
				}
			}
		}`),
	}
	_, err := o.client.ComponentTemplate.Create(ctx, createIndexComponentTemplateReq)
	if err != nil {
		log.Errorf("Error creating index component template %s: %s", o.dataStreamDefaultComponentTemplate, err)
		return err
	}
	log.Debugf("Index component template created for %s", o.dataStreamDefaultComponentTemplate)

	return nil
}

func (o *openSearchClient) createIndexTemplate(ctx context.Context, datastreamName string) error {
	createIndexTemplateReq := opensearchapi.IndexTemplateCreateReq{
		IndexTemplate: datastreamName,
		Body: strings.NewReader(fmt.Sprintf(`{
	"index_patterns": [
		"%s"
	],
	"composed_of": [
		"%s"
	],
	"priority": 200,
	"data_stream": {},
	"_meta": {
		"description": "default template"
	}
}`, datastreamName, o.dataStreamDefaultComponentTemplate)),
	}
	_, err := o.client.IndexTemplate.Create(ctx, createIndexTemplateReq)
	if err != nil {
		log.Errorf("Error creating index template %s: %s", datastreamName, err)
		return err
	}
	log.Debugf("Index template created for %s", datastreamName)

	return nil
}

func (o *openSearchClient) createDataStream(ctx context.Context, datastreamName string) error {
	createDataStreamReq := opensearchapi.DataStreamCreateReq{
		DataStream: datastreamName,
	}
	_, err := o.client.DataStream.Create(ctx, createDataStreamReq)
	if err != nil {
		log.Errorf("Error creating data stream %s: %s", datastreamName, err)
		return err
	}
	log.Debugf("Data stream created for %s", datastreamName)

	return nil
}

func (o *openSearchClient) bulkInsertDataStream(ctx context.Context, indexName, document string) (err error) {
	_, err = o.client.Bulk(
		ctx,
		opensearchapi.BulkReq{
			Body: strings.NewReader(fmt.Sprintf(`{ "index": { "_index": "%s" } }
%s
`, indexName, document)),
		},
	)
	if err != nil {
		log.Errorf("Error bulk inserting document into data stream %s: %s", indexName, err)
		return err
	}
	log.Debugf("Bulk insert successful for data stream %s", indexName)

	return nil
}

func (o *openSearchClient) OnFailure(f func(*pb.Timber)) {
	o.onFailureFunc = f
}

func (e *openSearchClient) WithRedactor(r Redactor) {
	e.redactor = r
}
