package elastic

import (
	"github.com/olivere/elastic/v7"
	cond "github.com/vela-ssoc/vela-cond"
	"github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/lua"
	vswitch "github.com/vela-ssoc/vela-switch"
)

func (c *Client) sendL(L *lua.LState) int {
	n := L.GetTop()
	if n == 0 {
		return 0
	}

	for i := 1; i <= n; i++ {
		c.Write(lua.S2B(L.Get(i).String()))
	}

	return 0
}

func (c *Client) indexL(L *lua.LState) int {
	n := L.GetTop()
	if n == 0 {
		L.RaiseError("set index fail got nil")
		return 0
	}

	format := L.CheckString(1)
	var fields []string

	for i := 2; i <= n; i++ {
		field := L.CheckString(i)
		if len(field) < 2 {
			continue
		}
		fields = append(fields, field)
	}

	c.index = PrepareIndex(format, fields)
	return 0
}

func (c *Client) dropL(L *lua.LState) int {
	cnd := cond.CheckMany(L)

	if len(c.drop) == 0 {
		c.drop = []*cond.Cond{cnd}
		return 0
	}

	c.drop = append(c.drop, cnd)
	return 0
}

func (c *Client) switchL(L *lua.LState) int {
	c.vsh = vswitch.CheckSwitch(L, 1)
	return 0
}

func (c *Client) startL(L *lua.LState) int {
	xEnv.Start(L, c).From(L.CodeVM()).Do()
	return 0
}

/*
	es.index("aabcc-%s" , "$day")
	res := es.search(index , "name:123" , "ab:aa" , "ac:123")


	app.count()

	app := es.

*/

func (c *Client) searchL(L *lua.LState) int {
	if !c.cfg.Default {
		L.RaiseError("not allow msearch only vela.elastic.default can msearch")
		return 0
	}

	n := L.GetTop()
	if n < 2 {
		L.RaiseError("invalid args , usage: es.search(index , query1 , query2)")
		return 0
	}

	index := L.CheckString(1)
	cli, err := EsApiClient()
	if err != nil {
		L.RaiseError("new elastic client fail %v", err)
		return 0
	}

	s := cli.Search(index).Size(100)
	for i := 2; i <= n; i++ {
		item := L.Get(i)
		if item.Type() != lua.LTString {
			continue
		}

		name, value := auxlib.ParamValue(item.String())
		if len(value) == 0 {
			L.RaiseError("search fail query args must be key:value , got %s", item.String())
			return 0
		}
		s.Query(elastic.NewTermQuery(name, value))
	}

	r, err := s.Do(L.Context())
	L.Push(&ElasticsearchResult{cli: cli, Err: err, Result: r, index: index})
	return 1
}

func (c *Client) withL(L *lua.LState) int {
	return 0
}

func (c *Client) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "start":
		return lua.NewFunction(c.startL)
	case "with":
		return lua.NewFunction(c.withL)
	case "search":
		return lua.NewFunction(c.searchL)
	case "send":
		return lua.NewFunction(c.sendL)
	case "index":
		return lua.NewFunction(c.indexL)
	case "drop":
		return lua.NewFunction(c.dropL)
	case "switch":
		return lua.NewFunction(c.switchL)
	}

	return lua.LNil
}
