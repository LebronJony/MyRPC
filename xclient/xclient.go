package xclient

import (
	. "MyRPC"
	"context"
	"io"
	"reflect"
	"sync"
)

/*
	向用户暴露一个支持负载均衡的客户端 XClient
*/

type XClient struct {
	// 服务发现实例
	d Discovery
	// 负载均衡模式
	mode SelectMode
	// 协议选项
	opt *Option
	mu  sync.Mutex
	// key 地址 value 客户端
	clients map[string]*Client
}

var _ io.Closer = (*XClient)(nil)

func NewXClient(d Discovery, mode SelectMode, opt *Option) *XClient {
	return &XClient{d: d, mode: mode, opt: opt, clients: make(map[string]*Client)}
}

func (xc *XClient) Close() error {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	for key, client := range xc.clients {
		_ = client.Close()
		delete(xc.clients, key)
	}
	return nil
}

// 将复用 Client 的能力封装在方法 dial 中
func (xc *XClient) dial(rpcAddr string) (*Client, error) {
	xc.mu.Lock()
	defer xc.mu.Unlock()
	// 检查 xc.clients 是否有缓存的 Client
	client, ok := xc.clients[rpcAddr]
	// 检查是否是可用状态，如果是则返回缓存的 Client，如果不可用，则从缓存中删除
	if ok && !client.IsAvailable() {
		_ = client.Close()
		delete(xc.clients, rpcAddr)
		client = nil
	}
	// 如果没有返回缓存的 Client，则说明需要创建新的 Client，缓存并返回
	if client == nil {
		var err error
		client, err = XDial(rpcAddr, xc.opt)
		if err != nil {
			return nil, err
		}
		xc.clients[rpcAddr] = client
	}
	return client, nil
}

func (xc *XClient) call(rpcAddr string, ctx context.Context, serviceMethod string, args, reply interface{}) error {
	client, err := xc.dial(rpcAddr)
	if err != nil {
		return err
	}
	return client.Call(ctx, serviceMethod, args, reply)
}

// Call 调用命名函数，等待它完成，并返回其错误状态。
// xc 会选择合适的服务器。
func (xc *XClient) Call(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	rpcAddr, err := xc.d.Get(xc.mode)
	if err != nil {
		return err
	}
	return xc.call(rpcAddr, ctx, serviceMethod, args, reply)
}

// Broadcast 广播为发现中注册的每个服务器调用命名函数
func (xc *XClient) Broadcast(ctx context.Context, serviceMethod string, args, reply interface{}) error {
	servers, err := xc.d.GetAll()
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var e error
	// 如果回复为零，则不需要设置值
	replyDone := reply == nil
	// WithCancel 返回带有新 Done 通道的 ctx 的副本。
	// cancel确保有错误发生时，快速失败
	ctx, cancel := context.WithCancel(ctx)
	for _, rpcAddr := range servers {
		wg.Add(1)
		go func(rpcAddr string) {
			defer wg.Done()
			var clonedReply interface{}
			if reply != nil {
				clonedReply = reflect.New(reflect.ValueOf(reply).Elem().Type()).Interface()
			}
			err := xc.call(rpcAddr, ctx, serviceMethod, args, clonedReply)
			mu.Lock()
			if err != nil && e == nil {
				e = err
				// 如果有任何调用失败，取消未完成的调用
				cancel()
			}
			if err == nil && !replyDone {
				reflect.ValueOf(reply).Elem().Set(reflect.ValueOf(clonedReply).Elem())
				replyDone = true
			}
			mu.Unlock()
		}(rpcAddr)
	}
	wg.Wait()
	return e
}

