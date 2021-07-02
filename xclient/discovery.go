package xclient

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

type SelectMode int

const (
	// RandomSelect 随机选择
	RandomSelect SelectMode = iota
	// RoundRobinSelect 使用罗宾算法选择
	RoundRobinSelect
)

type Discovery interface {
	// Refresh 从远程注册表更新服务列表
	Refresh() error
	// Update 手动更新服务列表
	Update(servers []string) error
	// Get 根据负载均衡策略，选择一个服务实例
	Get(mode SelectMode) (string, error)
	// GetAll 返回所有的服务实例
	GetAll()([]string, error)
}

// MultiServersDiscovery 是多服务器的发现，无需注册中心用户明确提供服务器地址
type MultiServersDiscovery struct {
	// r 是一个产生随机数的实例，初始化时使用时间戳设定随机数种子，避免每次产生相同的随机数序列
	r       *rand.Rand
	mu      sync.RWMutex
	servers []string
	// index 记录 Round Robin 算法已经轮询到的位置，为了避免每次从 0 开始，初始化时随机设定一个值
	index int
}

// NewMultiServerDiscovery 创建一个 MultiServersDiscovery 实例
func NewMultiServerDiscovery(servers []string) *MultiServersDiscovery {
	d := &MultiServersDiscovery{
		servers: servers,
		r:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	d.index = d.r.Intn(math.MaxInt32 - 1)
	return d
}

var _ Discovery = (*MultiServersDiscovery)(nil)

// Refresh 刷新对于 MultiServersDiscovery 没有意义，所以忽略它
func (d *MultiServersDiscovery) Refresh() error {
	return nil
}

// Update 如果需要，动态更新Discovery服务器
func (d *MultiServersDiscovery) Update(servers []string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.servers = servers
	return nil
}

// Get 根据模式获取服务器
func (d *MultiServersDiscovery) Get(mode SelectMode) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	n := len(d.servers)
	if n == 0 {
		return "", errors.New("rpc discovery: no available servers")
	}

	switch mode {
	case RandomSelect:
		return d.servers[d.r.Intn(n)], nil
	case RoundRobinSelect:
		// 服务器可以更新，所以模式 n 以确保安全
		s := d.servers[d.index%n]
		d.index = (d.index + 1) % n
		return s, nil
	default:
		return "", errors.New("rpc discovery: not supported select mode")
	}

}

// GetAll 返回Discovery中的所有服务器
func (d *MultiServersDiscovery) GetAll() ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	// 返回一个d.servers的拷贝
	servers := make([]string, len(d.servers), len(d.servers))
	copy(servers, d.servers)
	return servers, nil
}
