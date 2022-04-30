package ignite

import "github.com/amsokol/ignite-go-client/binary/v1"

type LazyCache struct {
	binary bool
	name   string
}

type LazyObject struct {
	dirhash   int64
	directory string
	name      string
	meta      []byte
}

func (obj *LazyObject) BuildIgnite() *ignite.ComplexObject {
	c1 := ignite.NewComplexObject("LazyObject")
	c1.Set("dirhash", obj.dirhash)
	c1.Set("directory", obj.directory)
	c1.Set("name", obj.name)
	c1.Set("meta", obj.meta)

	return &c1
}
