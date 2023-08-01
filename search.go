package elastic

import (
	"github.com/olivere/elastic/v7"
	"github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/lua"
)

/*
	local es = vela.elastic.default{}
	local re = es.search("aaa" , "abc" , "ccc").size(100).do()
*/

type search struct {
	cli   *elastic.Client
	size  int
	index []string
	query []elastic.Query
}

func (s *search) String() string                         { return "" }
func (s *search) Type() lua.LValueType                   { return lua.LTObject }
func (s *search) AssertFloat64() (float64, bool)         { return 0, false }
func (s *search) AssertString() (string, bool)           { return "", false }
func (s *search) AssertFunction() (*lua.LFunction, bool) { return nil, false }
func (s *search) Peek() lua.LValue                       { return s }

func (s *search) sizeL(L *lua.LState) int {
	n := L.IsInt(1)
	if n < 0 {
		s.size = 1
	} else {
		s.size = L.IsInt(1)
	}

	L.Push(s)
	return 1
}

func (s *search) queryL(L *lua.LState) int {
	n := L.GetTop()
	for i := 1; i <= n; i++ {
		item := L.Get(i)
		if item.Type() != lua.LTString {
			continue
		}

		name, value := auxlib.ParamValue(item.String())
		if len(value) == 0 {
			L.RaiseError("search fail query args must be key:value , got %s", item.String())
			return 0
		}
		s.query = append(s.query, elastic.NewTermQuery(name, value))
	}

	L.Push(s)
	return 1
}

func (s *search) doL(L *lua.LState) int {
	cli, err := EsApiClient()
	if err != nil {
		L.RaiseError("new elastic client fail %v", err)
		return 0
	}

	s.cli = cli

	srh := cli.Search(s.index...).Size(s.size)

	for _, q := range s.query {
		srh.Query(q)
	}

	r, err := srh.Do(L.Context())
	L.Push(&ElasticsearchResult{Err: err, Result: r, cli: cli})
	return 1
}

func (s *search) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "size":
		return lua.NewFunction(s.sizeL)
	case "filter":
		return lua.NewFunction(s.queryL)
	case "run":
		return lua.NewFunction(s.doL)
	default:
		return lua.LNil
	}
}

func newSearchL(L *lua.LState) int {
	s := &search{size: 100}
	L.Callback(func(value lua.LValue) (stop bool) {
		s.index = append(s.index, value.String())
		return false
	})

	L.Push(s)
	return 1
}
