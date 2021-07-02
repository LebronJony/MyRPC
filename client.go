package MyRPC

import (
	"MyRPC/codec"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// Call 封装了结构体 Call 来承载一次 RPC 调用所需要的信息
type Call struct {
	Seq uint64
	// 格式 "<service>.<method>"
	ServiceMethod string
	// 函数参数
	Args interface{}
	// 函数返回值
	Reply interface{}
	Error error
	// 为了支持异步调用,当调用结束时，会调用 call.done() 通知调用方
	Done chan *Call
}

func (call *Call) done() {
	call.Done <- call
}

// Client 代表一个 RPC Client。
// 一个客户端可能有多个未完成的调用，
// 一个客户端可能同时被多个 goroutine 使用。
type Client struct {
	// 消息的编解码器，和服务端类似，用来序列化将要发送出去的请求，以及反序列化接收到的响应
	cc  codec.Codec
	opt *Option
	// 是一个互斥锁，和服务端类似，为了保证请求的有序发送，即防止出现多个请求报文混淆
	sending sync.Mutex
	// 是每个请求的消息头，header 只有在请求发送时才需要，而请求发送是互斥的，
	// 因此每个客户端只需要一个，声明在 Client 结构体中可以复用
	header codec.Header
	mu     sync.Mutex
	// 用于给发送的请求编号，每个请求拥有唯一编号
	seq uint64
	// 存储未处理完的请求，键是编号，值是 Call 实例
	pending map[uint64]*Call
	// closing 和 shutdown 任意一个值置为 true，则表示 Client 处于不可用的状态，
	// 但 closing 是用户主动关闭的，即调用 Close 方法，
	// 而 shutdown 置为 true 一般是有错误发生。
	closing  bool
	shutdown bool
}

var _ io.Closer = (*Client)(nil)
var ErrShutdown = errors.New("connection is shut down")

// Close 关闭连接
func (client *Client) Close() error {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing {
		return ErrShutdown
	}
	client.closing = true
	return client.cc.Close()

}

// IsAvailable 如果客户端正常工作，则 IsAvailable 返回 true
func (client *Client) IsAvailable() bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	return !client.shutdown && !client.closing
}

// 将参数 call 添加到 client.pending 中，并更新 client.seq
func (client *Client) registerCall(call *Call) (uint64, error) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closing || client.shutdown {
		return 0, ErrShutdown
	}
	call.Seq = client.seq
	client.pending[call.Seq] = call
	client.seq++
	return call.Seq, nil
}

// 根据 seq，从 client.pending 中移除对应的 call，并返回
func (client *Client) removeCall(seq uint64) *Call {
	client.mu.Lock()
	defer client.mu.Unlock()
	call := client.pending[seq]
	delete(client.pending, seq)
	return call
}

// 服务端或客户端发生错误时调用，将 shutdown 设置为 true，
// 且将错误信息通知所有 pending 状态的 call
func (client *Client) terminateCalls(err error) {
	client.sending.Lock()
	defer client.sending.Unlock()
	client.mu.Lock()
	defer client.mu.Unlock()
	client.shutdown = true
	for _, call := range client.pending {
		call.Error = err
		call.done()
	}
}

func (client *Client) receive() {
	var err error
	for err == nil {

		var h codec.Header
		// 读取请求头
		if err = client.cc.ReadHeader(&h); err != nil {
			break
		}

		call := client.removeCall(h.Seq)

		switch {
		// call 不存在，可能是请求没有发送完整，或者因为其他原因被取消，但是服务端仍旧处理了
		case call == nil:
			err = client.cc.ReadBody(nil)
		// call 存在，但服务端处理出错，即 h.Error 不为空
		case h.Error != "":
			call.Error = fmt.Errorf(h.Error)
			err = client.cc.ReadBody(nil)
			call.done()
		// call 存在，服务端处理正常，那么需要从 body 中读取 Reply 的值
		default:
			err = client.cc.ReadBody(call.Reply)
			if err != nil {
				call.Error = errors.New("reading body " + err.Error())
			}
			call.done()
		}

	}

	// 发生错误，因此 terminateCalls 调用
	client.terminateCalls(err)
}

// NewClient 创建 Client 实例时，首先需要完成一开始的协议交换，即发送 Option 信息给服务端。
// 协商好消息的编解码方式之后，再创建一个子协程调用 receive() 接收响应。
func NewClient(conn net.Conn, opt *Option) (*Client, error) {
	f := codec.NewCodecFuncMap[opt.CodecType]
	if f == nil {
		err := fmt.Errorf("invalid codec type %s", opt.CodecType)
		log.Println("rpc client: codec error:", err)
		return nil, err
	}

	// 将options发送给服务端
	if err := json.NewEncoder(conn).Encode(opt); err != nil {
		log.Println("rpc client: options error", err)
		_ = conn.Close()
		return nil, err
	}
	return newClientCodec(f(conn), opt), nil
}

func newClientCodec(cc codec.Codec, opt *Option) *Client {
	client := &Client{
		// seq 以 1 开头，0 表示调用无效
		seq:     1,
		cc:      cc,
		opt:     opt,
		pending: make(map[uint64]*Call),
	}
	go client.receive()
	return client
}

func parseOptions(opts ...*Option) (*Option, error) {
	if len(opts) == 0 || opts[0] == nil {
		return DefaultOption, nil
	}
	if len(opts) != 1 {
		return nil, errors.New("number of options is more than 1")
	}

	opt := opts[0]
	opt.MagicNumber = DefaultOption.MagicNumber
	if opt.CodecType == "" {
		opt.CodecType = DefaultOption.CodecType
	}
	return opt, nil
}

func (client *Client) send(call *Call) {
	// 确保客户端能够发送一个完整请求
	client.sending.Lock()
	defer client.sending.Unlock()

	// 注册这次call
	seq, err := client.registerCall(call)
	if err != nil {
		call.Error = err
		call.done()
		return
	}

	// 准备请求头
	client.header.ServiceMethod = call.ServiceMethod
	client.header.Seq = seq
	client.header.Error = ""

	// 编码并发送请求
	if err := client.cc.Write(&client.header, call.Args); err != nil {
		call := client.removeCall(seq)
		// call 可能为 nil，通常表示 Write 部分失败，
		// 客户端收到响应并处理
		if call != nil {
			call.Error = err
			call.done()
		}
	}
}

// Go 异步调用该函数。是客户端暴露给用户的 RPC 服务调用接口
// 它返回表示调用的 Call 结构实例。
func (client *Client) Go(serviceMethod string, args, reply interface{}, done chan *Call) *Call {
	if done == nil {
		done = make(chan *Call, 10)
	} else if cap(done) == 0 {
		log.Panicln("rpc client: done channel is unbuffered")
	}

	call := &Call{
		ServiceMethod: serviceMethod,
		Args:          args,
		Reply:         reply,
		Done:          done,
	}
	client.send(call)
	return call
}

// Call 调用命名函数，等待它完成，
// 并返回其错误状态。是客户端暴露给用户的 RPC 服务调用接口
// Client.Call 的超时处理机制，使用 context 包实现，控制权交给用户，控制更为灵活
func (client *Client) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	call := client.Go(serviceMethod, args, reply, make(chan *Call, 1))
	select {
	// Done方法返回一个信道（channel），当Context被撤销或过期时，该信道是关闭的，
	// 即它是一个表示Context是否已关闭的信号。
	case <-ctx.Done():
		client.removeCall(call.Seq)
		return errors.New("rpc client: call failed: " + ctx.Err().Error())
	case call := <-call.Done:
		return call.Error
	}
}

type clientResult struct {
	client *Client
	err    error
}

type newClientFunc func(conn net.Conn, opt *Option) (client *Client, err error)

// 实现了一个超时处理的外壳 dialTimeout，这个壳将 NewClient 作为入参，在 2 个地方添加了超时处理的机制
func dialTimeout(f newClientFunc, network, address string, opts ...*Option) (client *Client, err error) {
	opt, err := parseOptions(opts...)
	if err != nil {
		return nil, err
	}

	// DialTimeout 的作用类似于 Dial，但有超时处理,如果连接创建超时，将返回错误
	conn, err := net.DialTimeout(network, address, opt.ConnectTimeout)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			_ = conn.Close()
		}
	}()

	ch := make(chan clientResult)

		// 使用子协程执行 NewClient，执行完成后则通过信道 ch 发送结果
	go func() {
		client, err := f(conn, opt)
		ch <- clientResult{client: client, err: err}
	}()
	if opt.ConnectTimeout == 0 {
		result := <-ch
		return result.client, result.err
	}

	select {
	/// 如果 time.After() 信道先接收到消息，则说明 NewClient 执行超时，返回错误
	case <-time.After(opt.ConnectTimeout):
		return nil, fmt.Errorf("rpc client: connect timeout: expect within %s", opt.ConnectTimeout)
	case result := <-ch:
		return result.client, result.err
	}

}

// Dial 连接到指定网络地址的RPC服务器,便于用户传入服务端地址，创建 Client 实例
func Dial(network, address string, opts ...*Option) (*Client, error) {
	return dialTimeout(NewClient, network, address, opts...)
}

// NewHTTPClient 通过 HTTP 作为传输协议新建一个 Client 实例
// 客户端要做的，发起 CONNECT 请求，检查返回状态码即可成功建立连接
func NewHTTPClient(conn net.Conn, opt *Option) (*Client, error) {
	_, _ = io.WriteString(conn, fmt.Sprintf("CONNECT %s HTTP/1.0\n\n", defaultRPCPath))
	// 在切换到 RPC 协议之前需要成功的 HTTP 响应
	// 读:bufio.NewReader(conn) 返回响应：&http.Request{Method: "CONNECT"}
	resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "CONNECT"})
	if err == nil && resp.Status == connected {
		// 通过 HTTP CONNECT 请求建立连接之后，后续的通信过程就交给 NewClient 了
		return NewClient(conn, opt)
	}
	if err == nil {
		err = errors.New("unexpected HTTP response: " + resp.Status)
	}
	return nil, err
}

// DialHTTP 连接到指定网络地址的 HTTP RPC 服务器，监听默认的 HTTP RPC 路径
func DialHTTP(network,address string,opts ...*Option) (*Client,error) {
	return dialTimeout(NewHTTPClient,network,address,opts...)
}

// XDial 根据第一个参数rpcAddr调用不同的函数连接到RPC服务器。
// rpcAddr 是表示 rpc 服务器的通用格式（protocol@addr），
// 例如 http@10.0.0.1:7001, tcp@10.0.0.1:9999, unix@/tmp/geerpc.sock
func XDial(rpcAddr string,opts ...*Option) (*Client,error) {
	parts := strings.Split(rpcAddr,"@")
	if len(parts) != 2 {
		return nil, fmt.Errorf("rpc client err: wrong format '%s', expect protocol@addr", rpcAddr)
	}
	protocol,addr := parts[0],parts[1]
	switch protocol {
	case "http":
		return DialHTTP("tcp",addr,opts...)
	default:
		// tcp、unix 或其他传输协议
		return Dial(protocol,addr,opts...)
	}
}