package elastic

import "github.com/olivere/elastic/v7"

func EsApiClient() (*elastic.Client, error) {
	doer, err := xEnv.Doer("/api/v1/broker/proxy/elastic")
	if err != nil {
		//L.RaiseError("new elastic tunnel client fail %v", err)
		return nil, err
	}

	cli, err := elastic.NewClient(elastic.SetHttpClient(doer))
	if err != nil {
		//L.RaiseError("new elastic client fail %v", err)
		return nil, err
	}

	return cli, nil
}
