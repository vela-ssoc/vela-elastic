package elastic

import (
	"encoding/json"
	cond "github.com/vela-ssoc/vela-cond"
	"github.com/vela-ssoc/vela-kit/auxlib"
	"github.com/vela-ssoc/vela-kit/kind"
	"github.com/vela-ssoc/vela-kit/strutil"
	"strconv"
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

func (d *doc) bulk() []byte {
	chunk, err := json.Marshal(d.data)
	if err != nil {
		xEnv.Errorf("elastic bulk marshal fail %v", err)
		return nil
	}

	enc := kind.NewJsonEncoder()
	enc.WriteByte('{')
	enc.Tab("index")
	enc.KV("_index", d.index)
	enc.KV("_type", "_doc")
	enc.End("}}")
	enc.Char('\n')
	enc.Copy(chunk)
	enc.Char('\n')
	return enc.Bytes()

}

func (d *doc) v(key string) interface{} {
	switch key {
	case "day":
		return time.Now().Format("2006-01-02")
	case "today":
		return strconv.Itoa(time.Now().Day())
	case "month":
		return time.Now().Format("2006-01")
	case "year":
		return time.Now().Format("2006")
	}

	return d.data[key]
}

func (d *doc) Field(key string) string {
	return strutil.String(d.v(key))
}

func (d *doc) Compare(key, val string, method cond.Method) bool {
	item := d.v(key)
	if item == nil {
		return method("nil", val)
	}

	v, err := auxlib.ToStringE(item)
	if err != nil {
		return false
	}
	return method(v, val)
}

func newDoc(data []byte) (*doc, error) {
	d := doc{action: ACCEPT}
	err := json.Unmarshal(data, &d.data)
	now := time.Now()
	if err != nil {
		d.data = map[string]interface{}{
			"@timestamp": now,
			"@error":     err.Error(),
			"message":    auxlib.B2S(data),
		}
	} else {
		d.data["@timestamp"] = now
	}

	return &d, nil
}
