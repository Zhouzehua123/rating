package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/search"
	"github.com/elastic/go-elasticsearch/v8/typedapi/core/update"
	"github.com/elastic/go-elasticsearch/v8/typedapi/some"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
)

func main() {
	// 连接elacticsearch
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://100.89.157.35:9200",
		},
	}

	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		fmt.Println("elasticsearch连接失败:", err)
		return
	}
	fmt.Println("elasticsearch连接成功:", client)
	// 创建索引
	//createIndex(client)
	// 索引文档
	//indexDocument(client)
	// 索引文档2
	//indexDocument2(client)
	//查询文档
	//getDocument(client, "2")
	// 搜索文档
	//searchDocument(client)
	//searchDocument2(client)
	// 聚合示例
	//aggregationDemo(client)
	// 更新文档
	//updateDocument(client)
	//updateDocument2(client)
	// 删除文档
	deleteDocument(client)
}
func createIndex(client *elasticsearch.TypedClient) {
	// 创建索引
	resp, err := client.Indices.Create("my-review-1").Do(context.Background())
	if err != nil {
		fmt.Printf("创建索引失败,err:%v\n", err)
		return
	}
	fmt.Printf("Acknowledged:%v\n", resp.Acknowledged)
}

// Review 评价数据
type Review struct {
	ID          int64     `json:"id"`
	UserID      int64     `json:"userID"`
	Score       uint8     `json:"score"`
	Content     string    `json:"content"`
	Tags        []Tag     `json:"tags"`
	Status      int       `json:"status"`
	PublishTime time.Time `json:"publishDate"`
}

// Tag 评价标签
type Tag struct {
	Code  int    `json:"code"`
	Title string `json:"title"`
}

// indexDocument 索引文档
func indexDocument(client *elasticsearch.TypedClient) {
	// 定义 document 结构体对象
	d1 := Review{
		ID:      1,
		UserID:  147982601,
		Score:   5,
		Content: "这是一个好评！",
		Tags: []Tag{
			{1000, "好评"},
			{1100, "物超所值"},
			{9000, "有图"},
		},
		Status:      2,
		PublishTime: time.Now(),
	}

	// 添加文档
	resp, err := client.Index("my-review-1").
		Id(strconv.FormatInt(d1.ID, 10)).
		Document(d1).
		Do(context.Background())
	if err != nil {
		fmt.Printf("indexing document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%#v\n", resp.Result)
}

// indexDocument2 索引文档
func indexDocument2(client *elasticsearch.TypedClient) {
	// 定义 document 结构体对象
	d1 := Review{
		ID:      2,
		UserID:  147982601,
		Score:   1,
		Content: "这是一个差评！",
		Tags: []Tag{
			{2000, "差评"},
		},
		Status:      2,
		PublishTime: time.Now(),
	}

	// 添加文档
	resp, err := client.Index("my-review-1").
		Id(strconv.FormatInt(d1.ID, 10)).
		Document(d1).
		Do(context.Background())
	if err != nil {
		fmt.Printf("indexing document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%#v\n", resp.Result)
}

// getDocument 获取文档
func getDocument(client *elasticsearch.TypedClient, id string) {
	resp, err := client.Get("my-review-1", id).
		Do(context.Background())
	if err != nil {
		fmt.Printf("get document by id failed, err:%v\n", err)
		return
	}
	fmt.Printf("fileds:%s\n", resp.Source_)
}

// searchDocument2 指定条件搜索文档
func searchDocument2(client *elasticsearch.TypedClient) {
	// 搜索content中包含好评的文档
	resp, err := client.Search().
		Index("my-review-1").
		Query(&types.Query{
			MatchPhrase: map[string]types.MatchPhraseQuery{
				"content": {Query: "好评"},
			},
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("search document failed, err:%v\n", err)
		return
	}
	fmt.Printf("total: %d\n", resp.Hits.Total.Value)
	// 遍历所有结果
	for _, hit := range resp.Hits.Hits {
		fmt.Printf("%s\n", hit.Source_)
	}
}

// searchDocument 搜索所有文档
func searchDocument(client *elasticsearch.TypedClient) {
	// 搜索文档
	resp, err := client.Search().
		Index("my-review-1").
		Query(&types.Query{
			MatchAll: &types.MatchAllQuery{},
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("search document failed, err:%v\n", err)
		return
	}
	fmt.Printf("total: %d\n", resp.Hits.Total.Value)
	// 遍历所有结果
	for _, hit := range resp.Hits.Hits {
		fmt.Printf("%s\n", hit.Source_)
	}
}

// aggregationDemo 聚合
func aggregationDemo(client *elasticsearch.TypedClient) {
	avgScoreAgg, err := client.Search().
		Index("my-review-1").
		Request(
			&search.Request{
				Size: some.Int(0),
				Aggregations: map[string]types.Aggregations{
					"avg_score": { // 将所有文档的 score 的平均值聚合为 avg_score
						Avg: &types.AverageAggregation{
							Field: some.String("score"),
						},
					},
				},
			},
		).Do(context.Background())
	if err != nil {
		fmt.Printf("aggregation failed, err:%v\n", err)
		return
	}
	fmt.Printf("avgScore:%#v\n", avgScoreAgg.Aggregations["avg_score"])
	if avgAgg, ok := avgScoreAgg.Aggregations["avg_score"].(*types.AvgAggregate); ok && avgAgg != nil && avgAgg.Value != nil {
		fmt.Printf("平均分数: %v\n", *avgAgg.Value)
	} else {
		fmt.Println("未获取到平均分数")
	}
}

// updateDocument 更新文档
func updateDocument(client *elasticsearch.TypedClient) {
	// 修改后的结构体变量
	d1 := Review{
		ID:      1,
		UserID:  147982601,
		Score:   5,
		Content: "这是一个修改后的好评！", // 有修改
		Tags: []Tag{ // 有修改
			{1000, "好评"},
			{9000, "有图"},
		},
		Status:      2,
		PublishTime: time.Now(),
	}

	resp, err := client.Update("my-review-1", "1").
		Doc(d1). // 使用结构体变量更新
		Do(context.Background())
	if err != nil {
		fmt.Printf("update document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%v\n", resp.Result)
}

// updateDocument2 更新文档
func updateDocument2(client *elasticsearch.TypedClient) {
	// 修改后的JSON字符串
	str := `{
					"id":1,
					"userID":147982601,
					"score":5,
					"content":"这是一个二次修改后的好评！",
					"tags":[
						{
							"code":1000,
							"title":"好评"
						},
						{
							"code":9000,
							"title":"有图"
						}
					],
					"status":2,
					"publishDate":"2023-12-10T15:27:18.219385+08:00"
				}`
	// 直接使用JSON字符串更新
	resp, err := client.Update("my-review-1", "1").
		Request(&update.Request{
			Doc: json.RawMessage(str),
		}).
		Do(context.Background())
	if err != nil {
		fmt.Printf("update document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%v\n", resp.Result)
}
// deleteDocument 删除 document
func deleteDocument(client *elasticsearch.TypedClient) {
	resp, err := client.Delete("my-review-1", "1").
		Do(context.Background())
	if err != nil {
		fmt.Printf("delete document failed, err:%v\n", err)
		return
	}
	fmt.Printf("result:%v\n", resp.Result)
}
