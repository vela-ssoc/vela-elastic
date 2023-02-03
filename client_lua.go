package elastic

import (
	cond "github.com/vela-ssoc/vela-cond"
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

func (c *Client) Index(L *lua.LState, key string) lua.LValue {
	switch key {
	case "start":
		return lua.NewFunction(c.startL)
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
