package MyRPC

/*
一般来说，涉及协议协商的这部分信息，需要设计固定的字节来传输的。
但是为了实现上更简单，GeeRPC 客户端固定采用 JSON 编码 Option，
后续的 header 和 body 的编码方式由 Option 中的 CodeType 指定，
服务端首先使用 JSON 解码 Option，然后通过 Option 的 CodeType 解码剩余的内容。即报文将以这样的形式发送：

| Option{MagicNumber: xxx, CodecType: xxx} | Header{ServiceMethod ...} | Body interface{} |
| <------      固定 JSON 编码      ------>  | <-------   编码方式由 CodeType 决定   ------->|

在一次连接中，Option 固定在报文的最开始，Header 和 Body 可以有多个，即报文可能是这样的。

| Option | Header1 | Body1 | Header2 | Body2 | ...
*/

/*
从接收到请求到回复还差以下几个步骤：
第一步，根据入参类型，将请求的 body 反序列化；
第二步，调用 service.call，完成方法调用；
第三步，将 reply 序列化为字节流，构造响应报文，返回
*/

import (
	"MyRPC/codec"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"
)

const MagicNumber = 0x3bef5c

type Option struct {
	// MagicNumber 标记这是一个 rpc 请求
	MagicNumber int
	// 客户端可以选择不同的编解码器来编码正文
	CodecType      codec.Type
	ConnectTimeout time.Duration
	HandleTimeout  time.Duration
}

var DefaultOption = &Option{
	MagicNumber:    MagicNumber,
	CodecType:      codec.GobType,
	ConnectTimeout: time.Second * 10,
}

// Server 代表rpc服务端
type Server struct {
	serviceMap sync.Map
}

func NewServer() *Server {
	return &Server{}
}

// DefaultServer 是一个默认的 Server 实例，主要为了用户使用方便
var DefaultServer = NewServer()

func Accept(lis net.Listener) { DefaultServer.Accept(lis) }

// Accept 接受侦听器上的连接并为每个传入连接提供请求。
func (server *Server) Accept(lis net.Listener) {
	// for 循环等待 socket 连接建立，并开启子协程处理，
	// 处理过程交给了 ServerConn 方法
	for {
		conn, err := lis.Accept()
		if err != nil {
			log.Println("rpc server:accept error:", err)
			return
		}
		// 并发
		go server.ServeConn(conn)
	}
}

// ServeConn 在单个连接上运行服务器。
// ServeConn 阻塞，服务连接直到客户端挂断。
func (server *Server) ServeConn(conn io.ReadWriteCloser) {
	defer func() { _ = conn.Close() }()
	var opt Option
	// 使用 json.NewDecoder 反序列化得到 Option 实例
	if err := json.NewDecoder(conn).Decode(&opt); err != nil {
		log.Println("rpc server: options error: ", err)
		return
	}

	// 检查 MagicNumber的值是否正确
	if opt.MagicNumber != MagicNumber {
		log.Printf("rpc server: invalid magic number %x", opt.MagicNumber)
		return
	}
	// 根据 CodeType 得到对应的消息编解码器函数
	f := codec.NewCodecFuncMap[opt.CodecType]
	// 检查 CodeType 的值是否正确
	if f == nil {
		log.Printf("rpc server: invalid codec type %s", opt.CodecType)
		return
	}

	server.serveCodec(f(conn), &opt)

}

// invalidRequest 是发生错误时响应 argv 的占位符
// struct{}{}是一个复合字面量，它构造了一个struct{}类型的值，该值也是空
var invalidRequest = struct{}{}

// 处理请求是并发的，但是回复请求的报文必须是逐个发送的，
// 并发容易导致多个回复报文交织在一起，客户端无法解析。
// 在这里使用锁(sending)保证
func (server *Server) serveCodec(cc codec.Codec, opt *Option) {
	// 确保发送完整的回复
	sending := new(sync.Mutex)
	// 等到所有请求处理完毕
	wg := new(sync.WaitGroup)

	// 在一次连接中，允许接收多个请求，即多个 request header 和 request body，
	// 因此这里使用了 for 无限制地等待请求的到来
	for {
		// 读取请求
		req, err := server.readRequest(cc)
		if err != nil {
			if req == nil {
				// 无法恢复，所以关闭连接
				break
			}
			req.h.Error = err.Error()
			// 回复请求
			server.sendResponse(cc, req.h, invalidRequest, sending)
			continue
		}

		wg.Add(1)
		// 并发处理请求
		go server.handleRequest(cc, req, sending, wg, opt.HandleTimeout)
	}
	wg.Wait()
	_ = cc.Close()
}

// request 存储了一个调用的所有信息
type request struct {
	// 请求头部
	h *codec.Header
	// 请求的参数和回复
	argv, replyv reflect.Value
	mtype        *methodType
	svc          *service
}

func (server *Server) readRequestHeader(cc codec.Codec) (*codec.Header, error) {
	var h codec.Header
	if err := cc.ReadHeader(&h); err != nil {
		if err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println("rpc server: read header error:", err)
		}
		return nil, err
	}
	return &h, nil
}

func (server *Server) readRequest(cc codec.Codec) (*request, error) {
	h, err := server.readRequestHeader(cc)
	if err != nil {
		return nil, err
	}

	req := &request{h: h}
	req.svc, req.mtype, err = server.findService(h.ServiceMethod)
	if err != nil {
		return req, err
	}
	// 通过 newArgv() 和 newReplyv() 两个方法创建出两个入参实例
	req.argv = req.mtype.newArgv()
	req.replyv = req.mtype.newReplyv()

	// 确保 argvi 是一个指针，ReadBody 需要一个指针作为参数
	argvi := req.argv.Interface()
	if req.argv.Type().Kind() != reflect.Ptr {
		argvi = req.argv.Addr().Interface()
	}

	// 通过 cc.ReadBody() 将请求报文反序列化为第一个入参 argv
	if err = cc.ReadBody(argvi); err != nil {
		log.Println("rpc server: read argv err:", err)
		return req, err
	}
	return req, nil
}

func (server *Server) sendResponse(cc codec.Codec, h *codec.Header, body interface{}, sending *sync.Mutex) {
	sending.Lock()
	defer sending.Unlock()
	if err := cc.Write(h, body); err != nil {
		log.Println("rpc server: write response error:", err)
	}
}

func (server *Server) handleRequest(cc codec.Codec, req *request, sending *sync.Mutex, wg *sync.WaitGroup, timeout time.Duration) {

	defer wg.Done()
	// 这里需要确保 sendResponse 仅调用一次，因此将整个过程拆分为 called 和 sent 两个阶段
	called := make(chan struct{})
	sent := make(chan struct{})
	go func() {
		// 通过 req.svc.call 完成方法调用
		err := req.svc.call(req.mtype, req.argv, req.replyv)
		called <- struct{}{}
		if err != nil {
			req.h.Error = err.Error()
			// 将 replyv 传递给 sendResponse 完成序列化
			server.sendResponse(cc, req.h, invalidRequest, sending)
			sent <- struct{}{}
			return
		}
		server.sendResponse(cc, req.h, req.replyv.Interface(), sending)
		sent <- struct{}{}
	}()

	if timeout == 0 {
		<-called
		<-sent
		return
	}

	select {
	// time.After() 先于 called 接收到消息，说明处理已经超时，called 和 sent 都将被阻塞。
	// 在 case <-time.After(timeout) 处调用 sendResponse
	case <-time.After(timeout):
		req.h.Error = fmt.Sprintf("rpc server: request handle timeout: expect within %s", timeout)
		server.sendResponse(cc, req.h, invalidRequest, sending)
	// called 信道接收到消息，代表处理没有超时，继续执行 sendResponse
	case <-called:
		<-sent
	}

}

// Register 注册在服务器中发布的方法集
// 满足以下条件的接收器值：
// - 导出类型的导出方法
// - 两个参数，都是导出类型
// - 第二个参数是一个指针
// - 一个返回值，类型错误
func (server *Server) Register(rcvr interface{}) error {
	s := newService(rcvr)
	// LoadOrStore 返回键的现有值（如果存在）。
	// 否则，它存储并返回给定的值。
	// 如果值已加载，则加载结果为 true，如果已存储，则为 false。
	if _, dup := server.serviceMap.LoadOrStore(s.name, s); dup {
		return errors.New("rpc: service already defined: " + s.name)
	}
	return nil
}

func Register(rcvr interface{}) error {
	return DefaultServer.Register(rcvr)
}

// 通过 ServiceMethod 从 serviceMap 中找到对应的 service
func (server *Server) findService(serviceMethod string) (svc *service, mtype *methodType, err error) {
	// LastIndex 返回 s 中 substr 最后一个实例的索引
	// 因为 ServiceMethod 的构成是 “Service.Method”，因此先将其分割成 2 部分，
	// 第一部分是 Service 的名称，第二部分即方法名。
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		err = errors.New("rpc server: service/method request ill-formed: " + serviceMethod)
		return
	}
	serviceName, methodName := serviceMethod[:dot], serviceMethod[dot+1:]
	// Load返回键对应的值
	// 在 serviceMap 中找到对应的 service 实例
	svci, ok := server.serviceMap.Load(serviceName)
	if !ok {
		err = errors.New("rpc server: can't find service " + serviceName)
		return
	}

	svc = svci.(*service)
	// 再从 service 实例的 method 中，找到对应的 methodType
	mtype = svc.method[methodName]
	if mtype == nil {
		err = errors.New("rpc server: can't find method " + methodName)
	}
	return
}

const (
	connected        = "200 Connected to Gee RPC"
	// 为后续 DEBUG 页面预留的地址
	defaultRPCPath   = "/_geeprc_"
	defaultDebugPath = "/debug/geerpc"
)

// ServeHTTP 实现了一个响应 RPC 请求的 http.Handler。
func (server *Server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if req.Method != "CONNECT" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = io.WriteString(w, "405 must CONNECT\n")
		return
	}

	// Hijack即接管 HTTP 连接的代码，所谓的接管 HTTP 连接是指这里接管了 HTTP 的 TCP 连接，
	// 也就是说 Golang 的内置 HTTP 库和 HTTPServer 库将不会管理这个 TCP 连接的生命周期，
	// 这个生命周期已经划给 Hijacker 了
	// 当不想使用内置服务器的HTTP协议实现时，请使用Hijack。
	// 一般在在创建连接阶段使用HTTP连接，后续自己完全处理connection。
	// 符合这样的使用场景的并不多，基于HTTP协议的rpc算一个，从HTTP升级到WebSocket也算一个
	conn,_,err := w.(http.Hijacker).Hijack()
	if err != nil {
		log.Print("rpc hijacking ",req.RemoteAddr,": ",err.Error())
	}
	_,_=io.WriteString(conn,"HTTP/1.0 "+connected+"\n\n")
	server.ServeConn(conn)
}


// HandleHTTP 在 rpcPath 上为 RPC 消息注册一个 HTTP 处理程序。
// 仍然需要调用 http.Serve()，通常在 go 语句中。
func (server *Server) HandleHTTP() {
	http.Handle(defaultRPCPath,server)
	http.Handle(defaultDebugPath,debugHTTP{server})
	log.Println("rpc server debug path:",defaultDebugPath)
}

func HandleHTTP() {
	DefaultServer.HandleHTTP()
}