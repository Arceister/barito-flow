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
	"github.com/opensearch-project/opensearch-go"
	"github.com/opensearch-project/opensearch-go/opensearchapi"
	log "github.com/sirupsen/logrus"
	"github.com/zekroTJA/timedmap"
)

var _ Elastic = (*openSearchClient)(nil)
var openSearchCounter = 0

type openSearchClient struct {
	client           *opensearch.Client
	onFailureFunc    func(*pb.Timber)
	onStoreFunc      func(ctx context.Context, indexName, document string) (err error)
	jsonMarshaler    *jsonpb.Marshaler
	indexExistsCache *timedmap.TimedMap

	numOfShards   int
	numOfReplicas int

	redactor Redactor
}

type openSearchConfig struct {
	numOfShards   int
	numOfReplicas int
}

func NewOpenSearchConfig(
	numOfShards int,
	numOfReplicas int,
) openSearchConfig {
	return openSearchConfig{
		numOfShards:   numOfShards,
		numOfReplicas: numOfReplicas,
	}
}

func NewOpenSearch(config openSearchConfig, urls []string, openSearchUsername string, openSearchPassword string, httpClient *http.Client) (*openSearchClient, error) {
	if httpClient == nil {
		httpClient = &http.Client{}
	}

	var c *opensearch.Client
	var err error
	if openSearchUsername == "" || openSearchPassword == "" {
		c, err = opensearch.NewClient(opensearch.Config{
			Addresses:            urls,
			Transport:            httpClient.Transport,
			MaxRetries:           0,
			EnableRetryOnTimeout: true,
		})
	} else {
		c, err = opensearch.NewClient(opensearch.Config{
			Addresses:            urls,
			Username:             openSearchUsername,
			Password:             openSearchPassword,
			MaxRetries:           0,
			EnableRetryOnTimeout: true,
		})
	}

	if err != nil {
		return nil, err
	}

	client := &openSearchClient{
		client:           c,
		jsonMarshaler:    &jsonpb.Marshaler{},
		indexExistsCache: timedmap.New(10 * time.Minute),
		redactor:         &DummyRedactor{},

		numOfShards:   config.numOfShards,
		numOfReplicas: config.numOfReplicas,
	}
	client.onStoreFunc = client.bulkInsert

	return client, nil
}

func (o *openSearchClient) Store(ctx context.Context, timber pb.Timber) (err error) {
	indexPrefix := timber.GetContext().GetEsIndexPrefix()
	indexName := fmt.Sprintf("%s-%s", indexPrefix, time.Now().Format("2006.01.02"))
	appSecret := timber.GetContext().GetAppSecret()

	for {
		if o.ensureIndexExists(ctx, indexPrefix, indexName) {
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

func (o *openSearchClient) bulkInsert(_ context.Context, indexName, document string) (err error) {
	bulkBody := fmt.Sprintf(`{ "index" : { "_index" : "%s" } }
%s
`, indexName, document)

	bulkReq := opensearchapi.BulkRequest{
		Body: strings.NewReader(bulkBody),
	}

	bulkResp, err := bulkReq.Do(context.Background(), o.client)
	if err != nil {
		log.Errorf("Failed to perform bulk insert to index %s: %v", indexName, err)
		return err
	}
	defer bulkResp.Body.Close()

	if bulkResp.StatusCode != 200 && bulkResp.StatusCode != 201 {
		log.Errorf("Bulk insert to index %s failed, status code: %d", indexName, bulkResp.StatusCode)
		return fmt.Errorf("bulk insert failed with status code: %d", bulkResp.StatusCode)
	}
	return nil
}

func (o *openSearchClient) ensureIndexExists(ctx context.Context, indexPrefix, indexName string) bool {
	indexCacheFound := o.indexExistsCache.GetValue(indexName)
	if indexCacheFound != nil {
		return true
	}

	existsReq := opensearchapi.IndicesExistsRequest{
		Index: []string{indexName},
	}

	existsResp, err := existsReq.Do(ctx, o.client)
	if err != nil {
		log.Errorf("Failed to check index existence for %s: %v", indexName, err)
		return false
	}
	defer existsResp.Body.Close()

	if existsResp.StatusCode == 200 {
		o.indexExistsCache.Set(indexName, true, 10*time.Minute)
		return true
	}

	if existsResp.StatusCode == 404 {
		if o.createIndex(ctx, indexName) {
			o.indexExistsCache.Set(indexName, true, 10*time.Minute)
			return true
		}
		return false
	}

	// TODO: apply ISM here if needed
	if err := o.createAndApplyISMIfNeeded(ctx, indexPrefix, indexName); err != nil {
		log.Errorf("Failed to create and apply ISM for index %s: %v", indexName, err)
		return false
	}

	log.Errorf("Unexpected status code when checking index %s: %d", indexName, existsResp.StatusCode)
	return false
}

func (o *openSearchClient) createAndApplyISMIfNeeded(_ context.Context, indexPrefix, indexName string) error {
	// Placeholder for ISM creation and application logic
	// Implement ISM creation and application as per your requirements
	return nil
}

func (o *openSearchClient) createIndex(ctx context.Context, indexName string) bool {
	settings := fmt.Sprintf(`{
		"settings": {
			"index": {
				"number_of_shards": %d,
				"number_of_replicas": %d
			}
		}
	}`, o.numOfShards, o.numOfReplicas)

	createReq := opensearchapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(settings),
	}

	createResp, err := createReq.Do(ctx, o.client)
	if err != nil {
		log.Errorf("Failed to create index %s: %v", indexName, err)
		return false
	}
	defer createResp.Body.Close()

	if createResp.StatusCode != 200 && createResp.StatusCode != 201 {
		log.Errorf("Failed to create index %s, status code: %d", indexName, createResp.StatusCode)
		return false
	}

	return true
}

func (o *openSearchClient) OnFailure(f func(*pb.Timber)) {
	o.onFailureFunc = f
}

func (e *openSearchClient) WithRedactor(r Redactor) {
	e.redactor = r
}
