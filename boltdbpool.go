package boltdbpool

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

var (
	DefaultFileMode     = 0644
	DefaultErrorHandler = func(err error) {
		log.Printf("error: %v", err)
	}
	DefaultCloseSleep = 250 * time.Millisecond
)

type Connection struct {
	DB *bolt.DB

	pool       *Pool
	path       string
	count      int64
	closeDelay time.Duration
	closeTime  time.Time
	mu         *sync.Mutex
}

func (c *Connection) Close() error {
	c.decrement()
	if c.count <= 0 {
		if c.closeDelay == 0 {
			return c.removeFromPool()
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		c.closeTime = time.Now().Add(c.closeDelay)
	}
	return nil
}

func (c *Connection) increment() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closeTime = time.Time{}
	c.count++
}

func (c *Connection) decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.count--
}

func (c *Connection) removeFromPool() error {
	return c.pool.remove(c.path)
}

type Options struct {
	BoltOptions  *bolt.Options
	FileMode     os.FileMode
	CloseDelay   time.Duration
	ErrorHandler func(err error)
}

type Pool struct {
	Options *Options

	errorChannel chan error
	connections  map[string]*Connection
	mu           *sync.Mutex
}

func (p *Pool) Close() {
	for _, c := range p.connections {
		p.errorChannel <- c.removeFromPool()
	}
	close(p.errorChannel)
}

func New(options *Options) *Pool {
	if options == nil {
		options = &Options{}
	}
	if options.FileMode == 0 {
		options.FileMode = os.FileMode(DefaultFileMode)
	}
	if options.ErrorHandler == nil {
		options.ErrorHandler = DefaultErrorHandler
	}
	p := &Pool{
		Options:      options,
		errorChannel: make(chan error),
		connections:  map[string]*Connection{},
		mu:           &sync.Mutex{},
	}
	go func() {
		for {
			for _, c := range p.connections {
				if !c.closeTime.IsZero() && c.closeTime.Before(time.Now()) {
					p.errorChannel <- c.removeFromPool()
				}
			}
			time.Sleep(DefaultCloseSleep)
		}
	}()
	go func() {
		for err := range p.errorChannel {
			if err != nil {
				p.Options.ErrorHandler(err)
			}
		}
	}()
	return p
}

func (p *Pool) Get(path string) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.connections[path]; ok {
		c.increment()
		return c, nil
	}
	db, err := bolt.Open(path, p.Options.FileMode, p.Options.BoltOptions)
	if err != nil {
		return nil, err
	}
	c := &Connection{
		DB:         db,
		path:       path,
		pool:       p,
		closeDelay: p.Options.CloseDelay,
		mu:         &sync.Mutex{},
	}
	p.connections[path] = c

	c.increment()
	return c, nil
}

func (p *Pool) Has(path string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.connections[path]
	return ok
}

func (p *Pool) remove(path string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	c, ok := p.connections[path]
	if !ok {
		return fmt.Errorf("boltdbpool: Unknown DB %s", path)
	}
	delete(p.connections, path)
	return c.DB.Close()
}
