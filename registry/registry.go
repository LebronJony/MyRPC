package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

// GeeRegistry 是一个简单的注册中心，提供以下功能。
// 添加一个服务器并接收心跳以使其保持活动状态。
// 返回所有活动服务器并同时删除死服务器同步。
type GeeRegistry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultPath = "/_geerpc_/registry"
	// 默认超时时间设置为 5 min，也就是说，任何注册的服务超过 5 min，即视为不可用状态。
	defaultTimeout = time.Minute * 5
)

// New 新建一个带有超时设置的注册表实例
func New(timeout time.Duration) *GeeRegistry {
	return &GeeRegistry{
		servers: make(map[string]*ServerItem),
		timeout: timeout,
	}
}

var DefaultGeeRegister = New(defaultTimeout)

// 添加服务实例，如果服务已经存在，则更新 start。
func (r *GeeRegistry) putServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.servers[addr]
	if s == nil {
		r.servers[addr] = &ServerItem{Addr: addr, start: time.Now()}
	} else {
		// 如果存在，更新开始时间以保持活动状态
		s.start = time.Now()
	}
}

// 返回可用的服务列表，如果存在超时的服务，则删除。
func (r *GeeRegistry) aliveServers() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for addr, s := range r.servers {
		if r.timeout == 0 || s.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, addr)
		} else {
			delete(r.servers, addr)
		}
	}
	sort.Strings(alive)
	return alive
}

// 为了实现上的简单，GeeRegistry 采用 HTTP 协议提供服务，且所有的有用信息都承载在 HTTP Header 中
func (r *GeeRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		// Get：返回所有可用的服务列表，通过自定义字段 X-Geerpc-Servers 承载。
		w.Header().Set("X-Geerpc-Servers", strings.Join(r.aliveServers(), ","))
	case "POST":
		// Post：添加服务实例或发送心跳，通过自定义字段 X-Geerpc-Server 承载。
		addr := req.Header.Get("X-Geerpc-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.putServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// HandleHTTP 在 registryPath 上为 GeeRegistry 消息注册一个 HTTP 处理程序
func (r *GeeRegistry) HandleHTTP(registryPath string) {
	http.Handle(registryPath, r)
	log.Println("rpc registry path:", registryPath)
}

func HandleHTTP() {
	DefaultGeeRegister.HandleHTTP(defaultPath)
}

// Heartbeat 每隔一段时间发送一次心跳消息，它是服务器注册或发送心跳的辅助函数
// 服务启动时定时向注册中心发送心跳，默认周期比注册中心设置的过期时间少 1 min
func Heartbeat(registry, addr string, duration time.Duration) {
	if duration == 0 {
		// 确保在从注册表中删除之前有足够的时间发送心跳
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}

	var err error
	err = sendHeartbeat(registry, addr)
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(registry, addr)
		}
	}()
}

func sendHeartbeat(registry, addr string) error {
	log.Println(addr, "send heart beat to registry", registry)
	httpClient := &http.Client{}
	req, _ := http.NewRequest("POST", registry, nil)
	req.Header.Set("X-Geerpc-Server", addr)
	// Do发送http请求
	if _, err := httpClient.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
