package estest

import (
	"context"
	"log"
	"reflect"
	"strconv"

	"github.com/davecgh/go-spew/spew"
	"github.com/olivere/elastic/v7"
	orderedmap "github.com/wk8/go-ordered-map"
)

// Data テストデータ
type Data struct {
	datamap *orderedmap.OrderedMap
}

// NewData データコンストラクタ
func NewData() *Data {
	return &Data{
		datamap: orderedmap.New(),
	}
}

// Set セット
func (m *Data) Set(key, value interface{}) *Data {
	m.datamap.Set(key, value)
	return m
}

// EsTest テストデータ
type EsTest struct {
	es      *elastic.Client
	indexes []string
}

// New コンストラクタ
func New(es *elastic.Client) *EsTest {
	return &EsTest{
		es: es,
	}
}

// CleaningIndexes defer時に削除したいテーブルを追加する
func (t *EsTest) CleaningIndexes(idx ...string) *EsTest {
	t.indexes = append(t.indexes, idx...)
	return t
}

// Exec データセットし、deferで使うclean関数を呼び出す
func (t *EsTest) Exec(ctx context.Context, d *Data) func() {
	if d == nil {
		return log.Fatal("data is empty")
	}
	for pair := d.datamap.Oldest(); pair != nil; pair = pair.Next() {
		idx := pair.Key.(string)
		t.insert(ctx, idx, pair.Value)
		t.indexes = append(t.indexes, idx)
	}
	return func() {
		t.clean(ctx, t.indexes...)
		t.indexes = []string{}
	}
}

// clean Tableデータのclear
func (t *EsTest) clean(ctx context.Context, idx ...string) {
	_, err := t.es.DeleteByQuery(idx...).
		Query(elastic.NewMatchAllQuery()).
		Refresh("true").
		ProceedOnVersionConflict().
		Do(ctx)
	if err != nil {
		log.Fatalf("clean() err = %+v\n", err)
	}
}

func (t *EsTest) insert(ctx context.Context, idx string, value interface{}) {
	var ifs []interface{}

	v := reflect.Indirect(reflect.ValueOf(value))

	switch v.Type().Kind() {
	case reflect.Slice:
		ifs := make([]interface{}, 0, v.Len())
		for i := 0; i < v.Len(); i++ {
			ifs = append(ifs, v.Index(i).Interface())
		}
	default:
		log.Fatalf("value type is unsupported")
		return
	}
	if len(ifs) == 0 {
		return
	}

	bs := t.es.Bulk()
	for _, a := range ifs {
		rv := reflect.ValueOf(a)
		if rv.Kind() == reflect.Ptr {
			rv = reflect.ValueOf(a).Elem()
		}

		var id string

		idField := rv.FieldByName("ID")
		switch idField.Kind() {
		case reflect.String:
			id = idField.String()
		case reflect.Int, reflect.Int32, reflect.Int64:
			id = strconv.FormatInt(idField.Int(), 10)
		default:
			log.Fatalf("type is unsupported")
		}

		bs = bs.Add(elastic.NewBulkIndexRequest().
			Index(idx).
			Id(id).
			RetryOnConflict(3).
			Doc(a))
	}

	if bs.NumberOfActions() == 0 {
		return
	}

	res, err := bs.Refresh("true").Do(ctx)
	if err != nil {
		log.Fatalf("insert err = %+v\n", err)
	}
	if res.Errors {
		log.Fatalf("%s", spew.Sdump(res.Items))
	}
}
