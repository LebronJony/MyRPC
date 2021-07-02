package codec

import "io"

// Header 我们将请求和响应中的参数和返回值抽象为 body，
// 剩余的信息放在 header 中，那么就可以抽象出数据结构 Header
type Header struct {
	// 服务名和方法名，通常与 Go 语言中的结构体和方法相映射
	ServiceMethod string
	// 请求的序号，也可以认为是某个请求的 ID，用来区分不同的请求
	Seq uint64
	// 错误信息，客户端置为空，服务端如果如果发生错误，将错误信息置于 Error 中
	Error string
}

// Codec 抽象出对消息体进行编解码的接口 Codec，
// 抽象出接口是为了实现不同的 Codec 实例
type Codec interface {
	io.Closer
	ReadHeader(*Header) error
	ReadBody(interface{}) error
	Write(*Header, interface{}) error
}

type NewCodecFunc func(io.ReadWriteCloser) Codec
type Type string

const (
	GobType  Type = "application/gob"
	JsonType Type = "application/json"
)

var NewCodecFuncMap map[Type]NewCodecFunc

func init()  {
	NewCodecFuncMap = make(map[Type]NewCodecFunc)
	NewCodecFuncMap[GobType] = NewGobCodec
}
