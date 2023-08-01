package elastic

import (
	"context"
	"encoding/json"
	"github.com/olivere/elastic/v7"
	"github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/kind"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/pipe"
)

type SearchHit struct {
	value *elastic.SearchHit
}

func (s *SearchHit) String() string                         { return auxlib.B2S(s.Byte()) }
func (s *SearchHit) Type() lua.LValueType                   { return lua.LTObject }
func (s *SearchHit) AssertFloat64() (float64, bool)         { return 0, false }
func (s *SearchHit) AssertString() (string, bool)           { return "", false }
func (s *SearchHit) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (s *SearchHit) Peek() lua.LValue                       { return s }

func (s *SearchHit) Byte() []byte {
	chunk, _ := json.Marshal(s.value)
	return chunk
}

func (s *SearchHit) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "id":
		return lua.S2L(s.value.Id)
	case "score":
		return lua.LNumber(*s.value.Score)
	case "index":
		return lua.S2L(s.value.Index)
	case "version":
		return lua.LNumber(*s.value.Version)
	case "source":
		fast := &kind.Fast{}
		err := fast.ParseBytes(s.value.Source)
		if err != nil {
			xEnv.Errorf("fast json decode source fail %v", err)
		}
		return fast
	}
	return lua.LNil
}

type ElasticsearchResult struct {
	offset int
	cli    *elastic.Client
	index  string
	Err    error `json:"err"`
	Result *elastic.SearchResult
}

func (ert *ElasticsearchResult) String() string                         { return auxlib.B2S(ert.Byte()) }
func (ert *ElasticsearchResult) Type() lua.LValueType                   { return lua.LTObject }
func (ert *ElasticsearchResult) AssertFloat64() (float64, bool)         { return 0, false }
func (ert *ElasticsearchResult) AssertString() (string, bool)           { return "", false }
func (ert *ElasticsearchResult) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (ert *ElasticsearchResult) Peek() lua.LValue                       { return ert }

func (ert *ElasticsearchResult) Byte() []byte {
	chunk, _ := json.Marshal(ert)
	return chunk
}

func (ert *ElasticsearchResult) Hit() int {
	if ert.Err != nil {
		return 0
	}
	if ert.Result == nil {
		return 0
	}
	return len(ert.Result.Hits.Hits)
}

func (ert *ElasticsearchResult) Total() int64 {
	if ert.Err != nil {
		return 0
	}
	if ert.Result == nil {
		return 0
	}
	return ert.Result.Hits.TotalHits.Value
}

func (ert *ElasticsearchResult) forEach(callback func(hit *SearchHit) bool) {
	n := len(ert.Result.Hits.Hits)
	if n == 0 {
		return
	}

	ert.offset = ert.offset + n

	for i := 0; i < n; i++ {
		hit := &SearchHit{value: ert.Result.Hits.Hits[i]}
		if !callback(hit) {
			return
		}
	}

}

func (ert *ElasticsearchResult) Scroll(ctx context.Context, callback func(hit *SearchHit) bool) {
	if ert.cli == nil {
		return
	}

	ert.forEach(callback)

	svc := ert.cli.Scroll(ert.index).Size(100).TrackTotalHits(true)
	for {
		xEnv.Errorf("scroll %s", ert.Result.ScrollId)
		result, err := svc.Do(ctx)
		if err != nil {
			xEnv.Errorf("Scroll request failed:", err)
			break
		}

		// 处理当前页的结果
		ert.Result = result

		// 如果没有更多结果，退出循环
		if len(result.Hits.Hits) == 0 {
			break
		}
		ert.forEach(callback)
		xEnv.Errorf("Offset:%d", ert.offset)
	}
}

func (ert *ElasticsearchResult) Ok() bool {
	if ert.Err != nil {
		return false
	}
	if ert.Result == nil {
		return false
	}

	return true
}

func (ert *ElasticsearchResult) pipe(L *lua.LState) int {
	if !ert.Ok() {
		return 0
	}

	n := len(ert.Result.Hits.Hits)
	if n == 0 {
		return 0
	}

	chains := pipe.NewByLua(L)
	co := xEnv.Clone(L)
	defer xEnv.Free(co)

	flag := false
	stop := lua.GoFuncErr(func(i ...interface{}) error {
		flag = true
		return nil
	})

	ert.forEach(func(hit *SearchHit) bool {
		err := chains.Call2(hit, stop, co)
		if err != nil {
			xEnv.Errorf("elastic search result pipe call fail %v", err)
		}
		return !flag //false stop ; true: continue
	})

	return 0
}

//es.search("elastic-index" , "name:123123").upsert(doc)

func (ert *ElasticsearchResult) upsertL(L *lua.LState) int {

	if ert.cli == nil {
		return 0
	}

	ert.Scroll(L.Context(), func(hit *SearchHit) bool {
		//xEnv.Errorf("%s", string(hit.value.Source))
		//_, e := ert.cli.Index().Index(hit.value.Index).Id(hit.value.Id).BodyString(`{"name":"vela" , "age":18 , "love":"security"}`).Refresh("true").Do(L.Context())
		//if e != nil {
		//	xEnv.Errorf("upsert doc fail %v", e)
		//}
		//xEnv.Infof("%v", r)
		return true
	})
	return 0
}

func (ert *ElasticsearchResult) scroll(L *lua.LState) int {
	if !ert.Ok() {
		return 0
	}

	n := len(ert.Result.Hits.Hits)
	if n == 0 {
		return 0
	}

	chains := pipe.NewByLua(L)
	co := xEnv.Clone(L)
	defer xEnv.Free(co)

	flag := false
	stop := lua.GoFuncErr(func(i ...interface{}) error {
		flag = true
		return nil
	})

	ert.Scroll(L.Context(), func(hit *SearchHit) bool {
		err := chains.Call2(hit, stop, co)
		if err != nil {
			xEnv.Errorf("elastic search result pipe call fail %v", err)
		}
		return !flag //false stop ; true: continue
	})
	return 0
}

func (ert *ElasticsearchResult) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "total":
		return lua.LNumber(ert.Total())
	case "size":
		return lua.LInt(ert.Hit())
	case "scroll":
		return lua.NewFunction(ert.scroll)
	case "pipe":
		return lua.NewFunction(ert.pipe)
	case "upsert":
		return lua.NewFunction(ert.upsertL)
	}

	return lua.LNil
}
