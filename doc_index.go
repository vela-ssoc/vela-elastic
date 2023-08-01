package elastic

import (
	"fmt"
	"github.com/vela-ssoc/vela-kit/lua"
)

func PrepareIndex(format string, fields []string) func(*doc) error { // evt-log-%s
	if len(fields) == 0 {
		return func(d *doc) error {
			d.index = format
			return nil
		}
	}

	return func(d *doc) error {
		var vals []interface{}
		for _, key := range fields {
			if key[0] != '$' {
				vals = append(vals, key)
				continue
			}

			val := d.v(key[1:])
			if val == nil {
				vals = append(vals, "nil")
			} else {
				vals = append(vals, val)
			}
		}

		d.index = fmt.Sprintf(format, vals...)
		return nil
	}
}

func PrepareIndexL(format string, fields []string) lua.GoFuncErr {
	fn := PrepareIndex(format, fields)

	goFunc := func(v ...interface{}) error {
		d, ok := v[0].(*doc)
		if !ok {
			return fmt.Errorf("invalid message")
		}

		return fn(d)
	}

	return goFunc
}
