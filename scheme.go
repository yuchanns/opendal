package opendal

type Schemer interface {
	Scheme() string
	Path() string
}
