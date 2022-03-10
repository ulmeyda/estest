# estest

test時ににelasticsearchにデータを挿入するヘルパー
※未完成

## example

```go

type TestData struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

t.Run("test", func(t *testing.T) {

	testData1 := []TestData{
		{ID: 1, Name: "aaa"},
		{ID: 2, Name: "bbb"},
	}

	insertData := estest.NewData().
		Set("index_name1", testData1).
		Set("index_name2", testData2)

	es, _ := elastic.NewClicent()
	defer estest.New(es).CleaningIndexes("index_name3").Exec(ctx, insertData)()
})

```
