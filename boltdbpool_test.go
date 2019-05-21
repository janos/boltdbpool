// Copyright (c) 2015 Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package boltdbpool

import (
	"io/ioutil"
	"os"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestNewPool(t *testing.T) {
	pool := New(nil)
	defer pool.Close()

	if pool.options.ConnectionExpires != 0 {
		t.Error("pool.options.ConnectionExpires is not 0")
	}
	if pool.options.FileMode != DefaultFileMode {
		t.Error("pool.options.FileMode is not DefaultFileMode")
	}
	if pool.options.BoltOptions != nil {
		t.Error("pool.options.BoltOptions is not nil")
	}
	if len(pool.connections) != 0 {
		t.Error("pool.connections is not empty")
	}
}

func TestNewPoolOptions(t *testing.T) {
	connectionExpires := time.Duration(10 * time.Second)
	fileMode := os.FileMode(0444)
	boltOptions := &bolt.Options{}

	pool := New(&Options{
		ConnectionExpires: connectionExpires,
		FileMode:          fileMode,
		BoltOptions:       boltOptions,
	})
	defer pool.Close()

	if pool.options.ConnectionExpires != connectionExpires {
		t.Error("pool.options.ConnectionExpires is not connectionExpires")
	}
	if pool.options.FileMode != fileMode {
		t.Error("pool.options.FileMode is not fileMode")
	}
	if pool.options.BoltOptions != boltOptions {
		t.Error("pool.options.BoltOptions is not boltOptions")
	}
	if len(pool.connections) != 0 {
		t.Error("pool.connections is not empty")
	}
}

func TestPoolClose(t *testing.T) {
	pool := New(nil)

	path1 := tempfile()
	path2 := tempfile()
	defer func() {
		if err := os.Remove(path1); err != nil {
			t.Error(err)
		}
		if err := os.Remove(path2); err != nil {
			t.Error(err)
		}
	}()

	if _, err := pool.Get(path1); err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	if _, err := pool.Get(path2); err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	if poolLen := len(pool.connections); poolLen != 2 {
		t.Errorf("pool.connections number of connections is not 2: %d", poolLen)
	}

	pool.Close()

	if len(pool.connections) != 0 {
		t.Error("pool.connections is not empty after pool.Close()")
	}
}

func TestPoolGetError(t *testing.T) {
	pool := New(nil)
	defer pool.Close()

	if _, err := pool.Get(os.DevNull); err == nil {
		t.Errorf("No error on opening invalid file %s", os.DevNull)
	}
}

func TestConnection(t *testing.T) {
	pool := New(nil)
	defer pool.Close()

	path := tempfile()
	defer func() {
		err := os.Remove(path)
		if err != nil {
			t.Error(err)
		}
	}()

	connection, err := pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}

	if poolLen := len(pool.connections); poolLen != 1 {
		t.Errorf("pool.connections number of connections is not 1: %d", poolLen)
	}
	if connection.count != 1 {
		t.Errorf("connection reference counter is not 1: %d", connection.count)
	}
	if connection.pool != pool {
		t.Errorf("connection.pool does not contain the pool it is in: %#v", connection.pool)
	}
	if !connection.closeTime.IsZero() {
		t.Errorf("connection.closeTime is not zero: %s", connection.closeTime)
	}
	if connection.pool.options.ConnectionExpires != 0 {
		t.Errorf("connection.pool.options.ConnectionExpires is not 0: %s", connection.pool.options.ConnectionExpires)
	}
	if dbPath := connection.DB.Path(); dbPath != path {
		t.Errorf("connection.DB.Path() (%s) != path (%s)", dbPath, path)
	}
	if connection.path != path {
		t.Errorf("connection.path (%s) != path (%s)", connection.path, path)
	}
	if c := pool.connections[path]; c != connection {
		t.Error("connection not found in pool.connections")
	}
	if !pool.Has(path) {
		t.Errorf("pool.Has returns false for path: %s", path)
	}

	connection.Close()

	if connection.count != 0 {
		t.Errorf("connection reference counter is not 0 after connection.Close(): %d", connection.count)
	}
	if len(pool.connections) != 0 {
		t.Error("pool.connections is not empty after connection.Close()")
	}
	if connection.DB.Path() != "" {
		t.Errorf("connection.DB.Path() is not blank after connection.Close()")
	}
	if pool.Has(path) {
		t.Errorf("pool.Has returns true for path after connection.Close(): %s", path)
	}
}

func TestConnectionCounter(t *testing.T) {
	pool := New(nil)
	defer pool.Close()

	path := tempfile()
	defer func() {
		err := os.Remove(path)
		if err != nil {
			t.Error(err)
		}
	}()

	connection, err := pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	if connection.count != 1 {
		t.Errorf("connection reference counter is not 1: %d", connection.count)
	}
	pool.Get(path)
	if connection.count != 2 {
		t.Errorf("connection reference counter is not 2: %d", connection.count)
	}
	pool.Get(path)
	if connection.count != 3 {
		t.Errorf("connection reference counter is not 3: %d", connection.count)
	}
	connection.Close()
	if connection.count != 2 {
		t.Errorf("connection reference counter is not 2: %d", connection.count)
	}
	pool.Get(path)
	if connection.count != 3 {
		t.Errorf("connection reference counter is not 3: %d", connection.count)
	}
	connection.Close()
	connection.Close()
	connection.Close()
	if connection.count != 0 {
		t.Errorf("connection reference counter is not 2: %d", connection.count)
	}
}

func TestExpires(t *testing.T) {
	connectionExpires := time.Duration(2 * time.Second)
	pool := New(&Options{
		ConnectionExpires: connectionExpires,
	})
	defer pool.Close()

	path := tempfile()
	defer func() {
		err := os.Remove(path)
		if err != nil {
			t.Error(err)
		}
	}()

	connection, err := pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	if connection.pool.options.ConnectionExpires != connectionExpires {
		t.Error("connection.pool.options.ConnectionExpires is not connectionExpires")
	}
	pool.Get(path)
	if connection.count != 2 {
		t.Errorf("connection reference counter is not 2: %d", connection.count)
	}
	connection.Close()
	if connection.count != 1 {
		t.Errorf("connection reference counter is not 1: %d", connection.count)
	}
	if !connection.closeTime.IsZero() && connection.count > 0 {
		t.Errorf("connection.closeTime is not zero after connection.Close() and connection.count > 0")
	}

	connection.Close()
	if connection.closeTime.IsZero() {
		t.Errorf("connection.closeTime is still zero after connection.Close() with expires option")
	}
	time.Sleep(connectionExpires + 100*time.Millisecond)
	pool.mu.RLock()
	if poolLen := len(pool.connections); poolLen != 0 {
		t.Errorf("pool.connections number of connections is not 0: %d; after connection.Close() with expires option and time.Sleep()", poolLen)
	}
	pool.mu.RUnlock()

	// New connection
	connection, err = pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	connection.Close()
	pool.Get(path)
	if !connection.closeTime.IsZero() {
		t.Errorf("connection.closeTime is not zero after connection.Close() and seconf connection.Get() with expires option")
	}
	connection.Close()
}

func TestErrorHandler(t *testing.T) {
	mu := &sync.Mutex{}
	var errorMarker error
	pool := New(&Options{
		ErrorHandler: func(err error) {
			mu.Lock()
			errorMarker = err
			mu.Unlock()
		},
		BoltOptions: &bolt.Options{
			Timeout: 1,
		},
	})
	defer pool.Close()

	path := tempfile()
	defer func() {
		err := os.Remove(path)
		if err != nil {
			t.Error(err)
		}
	}()

	connection, err := pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}

	pool.mu.Lock()
	delete(pool.connections, path)
	pool.mu.Unlock()

	connection.Close()
	time.Sleep(time.Second)
	mu.Lock()
	if errorMarker == nil {
		t.Error("Error is not propagated to ErrorHandler")
	}
	mu.Unlock()

	path2 := tempfile()
	defer func() {
		err := os.Remove(path2)
		if err != nil {
			t.Error(err)
		}
	}()

	connection, err = pool.Get(path2)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}

	connection.DB.Close()

	connection.Close()
	time.Sleep(time.Second)
	if errorMarker == nil {
		t.Error("Error is not propagated to ErrorHandler")
	}

}

func tempfile() string {
	f, _ := ioutil.TempFile("", "boltdbpool-")
	f.Close()
	os.Remove(f.Name())
	return f.Name()
}
