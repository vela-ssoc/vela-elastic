package elastic

import (
	"bytes"
	"context"
	"github.com/olivere/elastic/v7"
	cond "github.com/vela-ssoc/vela-cond"
	"github.com/vela-ssoc/vela-kit/lua"
	"github.com/vela-ssoc/vela-kit/pipe"
	vswitch "github.com/vela-ssoc/vela-switch"
	"reflect"
	"time"
)

var typeof = reflect.TypeOf((*Client)(nil)).String()

type Client struct {
	lua.SuperVelaData
	cfg   *config
	err   error
	index func(*doc) error

	lastE  time.Time
	esapi  *elastic.Client
	pip    *pipe.Chains
	vsh    *vswitch.Switch
	drop   []*cond.Cond
	queue  chan *elastic.BulkIndexRequest
	ctx    context.Context
	cancel context.CancelFunc
}

func (c *Client) Name() string {
	return c.cfg.Index
}

func (c *Client) Type() string {
	return typeof
}

func (c *Client) doBulk(v []elastic.BulkableRequest, cli *elastic.Client) error {
	n := len(v)
	if n == 0 {
		return nil
	}

	if !c.cfg.Default {
		bulk := cli.Bulk()
		bulk.Add(v...)
		_, err := bulk.Do(c.ctx)
		return err
	}

	cli, err := EsApiClient()
	if err != nil {
		return err
	}
	defer cli.Stop()

	bulk := cli.Bulk()
	bulk.Add(v...)
	_, err = bulk.Do(c.ctx)
	return err
}

func (c *Client) run(n int) {
	for i := 1; i <= n; i++ {
		t := NewThread(c.ctx, i, c.cfg, c.doBulk)
		go t.Accept(c.queue)
	}
}

func (c *Client) Start() error {
	c.constructor()

	if c.cfg.Thread < 3 {
		c.run(3)
	} else {
		c.run(c.cfg.Thread)
	}
	return nil
}

func (c *Client) Close() error {
	if c.cancel != nil {
		c.cancel()
	}

	if c.queue != nil {
		close(c.queue)
	}

	return nil
}

func (c *Client) DoDrop(d *doc) bool {
	n := len(c.drop)
	if n == 0 {
		return false
	}

	for i := 0; i < n; i++ {
		cnd := c.drop[i]
		if cnd.Match(d) {
			return true
		}
	}
	return false
}

func (c *Client) DoPipe(d *doc) {
	if c.pip == nil {
		return
	}

	c.pip.Do(d, nil, func(err error) {
		xEnv.Errorf("elastic client pipe call fail %v", err)
	})
}

func (c *Client) DoSwitch(d *doc) {
	if c.vsh == nil {
		return
	}

	c.vsh.Do(d)
}

func (c *Client) DefaultClient() *elastic.Client {
	if c.esapi != nil {
		return c.esapi
	}

	cli, err := EsApiClient()
	if err != nil {
		xEnv.Errorf("elastic default client create fail %v", err)
		return nil
	}

	c.esapi = cli
	return cli
}

func (c *Client) ByDefault(d *doc) {
	bulk := d.bulk()
	if len(bulk) == 0 {
		return
	}
	reader := bytes.NewReader(bulk)

	cli, _ := EsApiClient()
	cli.Bulk().Add()
	if e := xEnv.Oneway("/api/v1/broker/forward/elastic", reader, nil); e != nil { //Oneway
		xEnv.Errorf("push broker forward elastic fail %v", e)
	}
}

func (c *Client) Write(v []byte) (n int, err error) {
	d, err := newDoc(v)
	if err != nil {
		return 0, err
	}

	err = c.index(d)
	if err != nil {
		return 0, err
	}

	if c.DoDrop(d) {
		return 0, nil
	}

	c.DoPipe(d)

	c.DoSwitch(d)

	switch d.action {
	case DROP:
		return 0, nil
	case ACCEPT:
		c.queue <- elastic.NewBulkIndexRequest().Index(d.index).Doc(d.data)
		//if !c.cfg.Default {
		//	c.queue <- elastic.NewBulkIndexRequest().Index(d.index).Doc(d.data)
		//	return 0, nil
		//}
		////c.ByDefault(d)
		//c.Default(elastic.NewBulkIndexRequest().Index(d.index).Doc(d.data))
	}

	return
}

func (c *Client) PrepareIndex() {
	if c.index == nil {
		c.index = func(d *doc) error {
			d.index = c.cfg.Index
			return nil
		}
	}
}

func (c *Client) constructor() {
	c.PrepareIndex()

	ctx, cancel := context.WithCancel(context.Background())
	c.ctx = ctx
	c.cancel = cancel
	c.queue = make(chan *elastic.BulkIndexRequest, 4096)
}

func newClient(cfg *config) *Client {
	c := &Client{cfg: cfg}
	c.V(lua.VTInit, time.Now(), typeof)
	return c
}
