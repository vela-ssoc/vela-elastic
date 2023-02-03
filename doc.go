package elastic

import (
	sonic "github.com/bytedance/sonic"
	cond "github.com/vela-ssoc/vela-cond"
	auxlib2 "github.com/vela-ssoc/vela-kit/auxlib"
	"time"
)

const (
	ACCEPT uint8 = iota + 1
	DROP
)

type doc struct {
	action uint8
	index  string
	data   map[string]interface{}
}

func (d *doc) v(key string) interface{} {
	switch key {
	case "today":
		return time.Now().Format("2006-01-02")
	case "month":
		return time.Now().Format("2006-01")
	case "year":
		return time.Now().Format("2006")
	}

	return d.data[key]
}

func (d *doc) Compare(key, val string, method cond.Method) bool {
	item := d.v(key)
	if item == nil {
		return method("nil", val)
	}

	v, err := auxlib2.ToStringE(item)
	if err != nil {
		return false
	}
	return method(v, val)
}

func newDoc(data []byte) (*doc, error) {
	d := doc{action: ACCEPT}
	err := sonic.Unmarshal(data, &d.data)
	now := time.Now()
	if err != nil {
		d.data = map[string]interface{}{
			"@timestamp": now,
			"@error":     err.Error(),
			"message":    auxlib2.B2S(data),
		}
	} else {
		d.data["@timestamp"] = now
	}

	return &d, nil
}
