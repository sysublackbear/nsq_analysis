package nsqlookupd

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type RegistrationDB struct {
	sync.RWMutex  // 读写锁
	registrationMap map[Registration]ProducerMap  // 注册表
	// 一个Topic或Channel对应多个Producers
}

// 注册表的key
type Registration struct {
	Category string
	Key      string
	SubKey   string
}
type Registrations []Registration

type PeerInfo struct {
	lastUpdate       int64                              // 上次更新时间
	id               string                             // 唯一ID表示这个Producer
	RemoteAddress    string `json:"remote_address"`     // Producer的远程地址
	Hostname         string `json:"hostname"`           // Producer的主机名字
	BroadcastAddress string `json:"broadcast_address"`  // Producer的广播地址
	TCPPort          int    `json:"tcp_port"`           // Producer的tcp端口
	HTTPPort         int    `json:"http_port"`          // Producer的http端口
	Version          string `json:"version"`            // 版本
}

type Producer struct {
	peerInfo     *PeerInfo  // 存放Producer信息，用一个指针指向其内存地址
	tombstoned   bool
	tombstonedAt time.Time
}

// 生产者列表
type Producers []*Producer
type ProducerMap map[string]*Producer

func (p *Producer) String() string {
	return fmt.Sprintf("%s [%d, %d]", p.peerInfo.BroadcastAddress, p.peerInfo.TCPPort, p.peerInfo.HTTPPort)
}

func (p *Producer) Tombstone() {
	p.tombstoned = true
	p.tombstonedAt = time.Now()
}

func (p *Producer) IsTombstoned(lifetime time.Duration) bool {
	return p.tombstoned && time.Now().Sub(p.tombstonedAt) < lifetime
}

func NewRegistrationDB() *RegistrationDB {
	return &RegistrationDB{
		registrationMap: make(map[Registration]ProducerMap),
	}
}

// add a registration key
func (r *RegistrationDB) AddRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
}

// add a producer to a registration
func (r *RegistrationDB) AddProducer(k Registration, p *Producer) bool {
	r.Lock()
	defer r.Unlock()
	_, ok := r.registrationMap[k]
	if !ok {
		r.registrationMap[k] = make(map[string]*Producer)
	}
	producers := r.registrationMap[k]
	_, found := producers[p.peerInfo.id]
	if found == false {
		producers[p.peerInfo.id] = p
	}
	return !found
}

// remove a producer from a registration
func (r *RegistrationDB) RemoveProducer(k Registration, id string) (bool, int) {
	r.Lock()
	defer r.Unlock()
	producers, ok := r.registrationMap[k]
	if !ok {
		return false, 0
	}
	removed := false
	if _, exists := producers[id]; exists {
		removed = true
	}

	// Note: this leaves keys in the DB even if they have empty lists
	delete(producers, id)
	return removed, len(producers)
}

// remove a Registration and all it's producers
func (r *RegistrationDB) RemoveRegistration(k Registration) {
	r.Lock()
	defer r.Unlock()
	delete(r.registrationMap, k)
}

func (r *RegistrationDB) needFilter(key string, subkey string) bool {
	return key == "*" || subkey == "*"
}

// todo: 这个函数的用途
func (r *RegistrationDB) FindRegistrations(category string, key string, subkey string) Registrations {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		if _, ok := r.registrationMap[k]; ok {
			return Registrations{k}
		}
		return Registrations{}
	}
	results := Registrations{}
	for k := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {  // 找出符合category, key, subkey相等所有的key
			continue
		}
		results = append(results, k)  // 追加到列表后面
	}
	return results
}

func (r *RegistrationDB) FindProducers(category string, key string, subkey string) Producers {
	r.RLock()
	defer r.RUnlock()
	if !r.needFilter(key, subkey) {
		k := Registration{category, key, subkey}
		return ProducerMap2Slice(r.registrationMap[k])  // 一定存在吗?由上游保证吗?
	}

	results := make(map[string]struct{})
	var retProducers Producers
	for k, producers := range r.registrationMap {
		if !k.IsMatch(category, key, subkey) {
			continue
		}
		for _, producer := range producers {
			_, found := results[producer.peerInfo.id]
			if found == false {
				results[producer.peerInfo.id] = struct{}{}
				retProducers = append(retProducers, producer)
			}
		}
	}
	return retProducers
}

func (r *RegistrationDB) LookupRegistrations(id string) Registrations {
	r.RLock()
	defer r.RUnlock()
	results := Registrations{}
	for k, producers := range r.registrationMap {
		if _, exists := producers[id]; exists {
			results = append(results, k)  // 将整个key加进去
		}
	}
	return results
}

func (k Registration) IsMatch(category string, key string, subkey string) bool {
	if category != k.Category {
		return false
	}
	if key != "*" && k.Key != key {
		return false
	}
	if subkey != "*" && k.SubKey != subkey {
		return false
	}
	return true
}

func (rr Registrations) Filter(category string, key string, subkey string) Registrations {
	output := Registrations{}
	for _, k := range rr {
		if k.IsMatch(category, key, subkey) {
			output = append(output, k)
		}
	}
	return output
}

func (rr Registrations) Keys() []string {
	keys := make([]string, len(rr))
	for i, k := range rr {
		keys[i] = k.Key
	}
	return keys
}

func (rr Registrations) SubKeys() []string {
	subkeys := make([]string, len(rr))
	for i, k := range rr {
		subkeys[i] = k.SubKey
	}
	return subkeys
}

func (pp Producers) FilterByActive(inactivityTimeout time.Duration, tombstoneLifetime time.Duration) Producers {
	now := time.Now()
	results := Producers{}
	for _, p := range pp {
		cur := time.Unix(0, atomic.LoadInt64(&p.peerInfo.lastUpdate))
		if now.Sub(cur) > inactivityTimeout || p.IsTombstoned(tombstoneLifetime) {
			continue
		}
		results = append(results, p)
	}
	return results
}

func (pp Producers) PeerInfo() []*PeerInfo {
	results := []*PeerInfo{}
	for _, p := range pp {
		results = append(results, p.peerInfo)
	}
	return results
}

func ProducerMap2Slice(pm ProducerMap) Producers {
	var producers Producers
	for _, producer := range pm {
		producers = append(producers, producer)
	}

	return producers
}
