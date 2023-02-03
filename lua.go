package elastic

import (
	"github.com/vela-ssoc/vela-kit/vela"
	"github.com/vela-ssoc/vela-kit/lua"
)

var xEnv vela.Environment

/*
local index = vale.elastic.index

local cli = vela.elastic.cli{
	url = "http://localhost:9092",
}

cli.index("%s-app-%s" , "$day" , "app")
cli.drop("host = www.baidu.com")
cli.drop("")

a._{
	["app = "www.baidu.com"] = index("app-%s-%s" , "$name" , "$app")
	["app = "www.baidu.com"] = index("app-%s-%s" , "$name" , "$app")
}

cli.switch(a)

local c = vela.kfk.consumer{}



cli.index("app-now)

local app = cli.clone("index-app")



*/

func newLuaClient(L *lua.LState) int {
	cfg := newConfig(L)
	proc := L.NewVelaData(cfg.name(), typeof)
	proc.Set(newClient(cfg))
	L.Push(proc)
	return 1
}

func newLuaIndexL(L *lua.LState) int {
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

	L.Push(PrepareIndexL(format, fields))
	return 1

}

func newLuaDropL(L *lua.LState) int {
	L.Push(lua.GoFuncErr(func(v ...interface{}) error {
		d, ok := v[0].(*doc)
		if ok {
			d.action = DROP
		}
		return nil
	}))
	return 1
}

func WithEnv(env vela.Environment) {
	xEnv = env
	es := lua.NewUserKV()
	es.Set("client", lua.NewFunction(newLuaClient))
	es.Set("index", lua.NewFunction(newLuaIndexL))
	es.Set("drop", lua.NewFunction(newLuaDropL))
	xEnv.Set("elastic", es)
}
