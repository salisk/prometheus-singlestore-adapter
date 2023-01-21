package reader

import "github.com/prometheus/prometheus/prompb"

type Reader interface {
	Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error)
	Name() string
	HealthCheck() error
}
