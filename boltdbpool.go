// Copyright (c) 2015 Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
Package boltdbpool implements a pool container for BoltDB github.com/coreos/bbolt databases.
Pool elements called connections keep reference counts for each database to close it
when it when the count is 0. Database is reused or opened based on database file path. Closing
the database must not be done directly, instead Connection.Close() method should be used.
Database is removed form the pool and closed by the goroutine in the background in respect to
reference count and delay in time if it is specified.

Example:

    package main

    import (
        "fmt"
        "time"

        "resenje.org/boltdbpool"
    )

    func main() {
        pool := boltdbpool.New(&boltdbpool.Options{
            ConnectionExpires: 5 * time.Second,
            ErrorHandler: boltdbpool.ErrorHandlerFunc(func(err error) {
                fmt.Printf("error: %v", err)
            }),
        })
        defer p.Close()

        ...

        c, err := pool.Get("/tmp/db.bolt")
        if err != nil {
            panic(err)
        }
        defer c.Close()

        ...

        c.DB.Update(func(tx *bolt.TX) error {
            ...
        })
    }
*/
package boltdbpool // import "resenje.org/boltdbpool"

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	// DefaultFileMode is used in bolt.Open() as file mode for database file
	// if FileMode is not specified in boltdbpool.Options.
	DefaultFileMode = os.FileMode(0666)

	// DefaultDirMode is used in os.MkdirAll() as file mode for database directories
	// if DirMode is not specified in boltdbpool.Options.
	DefaultDirMode = os.FileMode(0777)

	// DefaultErrorHandler accepts errors from
	// goroutine that closes the databases if ErrorHandler is not specified in
	// boltdbpool.Options.
	DefaultErrorHandler = ErrorHandlerFunc(func(err error) {
		log.Printf("error: %v", err)
	})
)

// Options are used when a new pool is created that.
type Options struct {
	// BoltOptions is used on bolt.Open().
	BoltOptions *bolt.Options

	// FileMode is used in bolt.Open() as file mode for database file. Default: 0640.
	FileMode os.FileMode

	// DirMode is used in os.MkdirAll() as file mode for database directories. Default: 0750.
	DirMode os.FileMode

	// ConnectionExpires is a duration between the reference count drops to 0 and
	// the time when the database is closed. It is useful to avoid frequent
	// openings of the same database. If the value is 0 (default), no caching is done.
	ConnectionExpires time.Duration

	// ErrorHandler represents interface that accepts errors from goroutine that closes the databases.
	ErrorHandler ErrorHandler
}

// Pool keeps track of connections.
type Pool struct {
	options       *Options
	connections   map[string]*Connection
	mu            sync.RWMutex
	removeTrigger chan struct{}
	quit          chan struct{}
}

// New creates new pool with provided options and also starts database closing goroutone
// and goroutine for errors handling to ErrorHandler.
func New(options *Options) *Pool {
	if options == nil {
		options = &Options{}
	}
	if options.FileMode == 0 {
		options.FileMode = DefaultFileMode
	}
	if options.DirMode == 0 {
		options.DirMode = DefaultDirMode
	}
	if options.ErrorHandler == nil {
		options.ErrorHandler = DefaultErrorHandler
	}
	p := &Pool{
		options:       options,
		connections:   map[string]*Connection{},
		removeTrigger: make(chan struct{}, 1),
		quit:          make(chan struct{}),
	}
	go func() {
		for {
			select {
			case <-p.removeTrigger:
				select {
				case <-time.After(p.options.ConnectionExpires):
				case <-p.quit:
					return
				}
				p.mu.Lock()
				for _, c := range p.connections {
					c.mu.RLock()
					if !c.closeTime.IsZero() && c.closeTime.Before(time.Now()) {
						p.handleError(c.removeFromPool())
					}
					c.mu.RUnlock()
				}
				p.mu.Unlock()
			case <-p.quit:
				return
			}
		}
	}()
	return p
}

// Get returns a connection that contains a database or creates a new connection
// with newly opened database based on options specified on pool creation.
func (p *Pool) Get(path string) (*Connection, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if c, ok := p.connections[path]; ok {
		c.mu.Lock()
		c.increment()
		c.mu.Unlock()
		return c, nil
	}
	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(path), p.options.DirMode); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	db, err := bolt.Open(path, p.options.FileMode, p.options.BoltOptions)
	if err != nil {
		return nil, err
	}
	c := &Connection{
		DB:   db,
		path: path,
		pool: p,
	}
	c.mu.Lock()
	c.increment()
	p.connections[path] = c
	c.mu.Unlock()
	return c, nil
}

// Has returns true if a database with a file path is in the pool.
func (p *Pool) Has(path string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, ok := p.connections[path]
	return ok
}

// Close function closes and removes from the pool all databases. After the execution
// pool is not usable.
func (p *Pool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, c := range p.connections {
		p.handleError(c.removeFromPool())
	}
	close(p.quit)
}

func (p *Pool) remove(path string) error {
	c, ok := p.connections[path]
	if !ok {
		return fmt.Errorf("boltdbpool: Unknown DB %s", path)
	}
	delete(p.connections, path)
	return c.DB.Close()
}

func (p *Pool) handleError(err error) {
	if err != nil {
		p.options.ErrorHandler.HandleError(err)
	}
}

// Connection encapsulates bolt.DB and keeps reference counter and closing time information.
type Connection struct {
	DB *bolt.DB

	pool      *Pool
	path      string
	count     int64
	closeTime time.Time
	mu        sync.RWMutex
}

// Close function on Connection decrements reference counter and closes the database if needed.
func (c *Connection) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.decrement()

	if c.count > 0 {
		return
	}

	if c.pool.options.ConnectionExpires == 0 {
		c.pool.mu.Lock()
		c.pool.handleError(c.removeFromPool())
		c.pool.mu.Unlock()
		return
	}

	c.closeTime = time.Now().Add(c.pool.options.ConnectionExpires)
	select {
	case c.pool.removeTrigger <- struct{}{}:
	default:
	}
}

func (c *Connection) increment() {
	// Reset the closing time
	c.closeTime = time.Time{}
	c.count++
}

func (c *Connection) decrement() {
	c.count--
}

func (c *Connection) removeFromPool() error {
	return c.pool.remove(c.path)
}

// ErrorHandler interface can be used for objects that log or panic on error
type ErrorHandler interface {
	HandleError(err error)
}

// The ErrorHandlerFunc type is an adapter to allow the use of
// ordinary functions as error handlers.
type ErrorHandlerFunc func(err error)

// HandleError calls f(err).
func (f ErrorHandlerFunc) HandleError(err error) {
	f(err)
}
