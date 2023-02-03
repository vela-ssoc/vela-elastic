package elastic

import (
	"github.com/olivere/elastic/v7"
	"golang.org/x/net/context"
	"time"
)

type Handler func([]elastic.BulkableRequest, *elastic.Client) error

type Thread struct {
	ID     int
	cfg    *config
	ctx    context.Context
	count  int
	handle Handler
	cli    *elastic.Client
	bucket []elastic.BulkableRequest
}

func NewThread(ctx context.Context, id int, cfg *config, hd Handler) *Thread {
	th := &Thread{
		ID:     id,
		cfg:    cfg,
		ctx:    ctx,
		handle: hd,
	}
	th.constructor()

	return th
}

func (th *Thread) constructor() {
	opt, err := th.cfg.OptionsFunc()
	if err != nil {
		xEnv.Errorf("%s elastic thread.id=%d client invalid option %v", th.cfg.name(), th.ID, err)
		return
	}

	cli, err := elastic.NewClient(opt...)
	if err != nil {
		xEnv.Errorf("%s elastic thread.id=%d client construct fail %v", th.cfg.name(), th.ID, err)
		return
	}

	th.cli = cli
}

func (th *Thread) Send() {
	th.handle(th.bucket, th.cli)
	th.bucket = th.bucket[:0]
	//xEnv.Errorf("thread cap=%d len=%d", cap(th.bucket), len(th.bucket))
}

func (th *Thread) append(r *elastic.BulkIndexRequest) {
	th.bucket = append(th.bucket, r)
	th.count++

	if len(th.bucket) < th.cfg.Flush {
		return
	}

	th.Send()
}

func (th *Thread) Accept(bch chan *elastic.BulkIndexRequest) {
	tk := time.NewTicker(time.Duration(th.cfg.Interval) * time.Second)
	defer func() {
		tk.Stop()
		th.cli.Stop()
	}()

	for {
		select {
		case <-th.ctx.Done():
			xEnv.Errorf("%s elastic.thread=%d exit..", th.cfg.name(), th.ID)
			return
		case r := <-bch:
			th.append(r)
		case <-tk.C:
			th.Send()
		}
	}
}
