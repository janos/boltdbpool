package boltdbpool

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/boltdb/bolt"
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

	defer func() {
		if err := recover(); err == nil {
			t.Error("No panic on closing pool.errorChannel after pool.Close(), the cannel was open")
		}
	}()

	close(pool.errorChannel)
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
	if connection.expires != 0 {
		t.Errorf("connection.expires is not 0: %s", connection.expires)
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
	if connection.expires != connectionExpires {
		t.Error("connection.xpires is not connectionExpires")
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
		t.Errorf("connection.closeTime is not zero after conection.Close() and connection.count > 0")
	}

	connection.Close()
	if connection.closeTime.IsZero() {
		t.Errorf("connection.closeTime is still zero after conection.Close() with expires option")
	}
	if dbPath := connection.DB.Path(); dbPath != path {
		t.Errorf("connection.DB.Path() (%s) != path (%s); after connection.Close() with expires option", dbPath, path)
	}
	time.Sleep(connectionExpires)
	time.Sleep(defaultCloseSleep)
	if poolLen := len(pool.connections); poolLen != 0 {
		t.Errorf("pool.connections number of connections is not 0: %d; after connection.Close() with expires option and time.Sleep()", poolLen)
	}
	if dbPath := connection.DB.Path(); dbPath == path {
		t.Error("database is still open after connection.Close() with expires option and time.Sleep()")
	}

	// New connection
	connection, err = pool.Get(path)
	if err != nil {
		t.Errorf("Getting new connection: %s", err)
	}
	connection.Close()
	pool.Get(path)
	if !connection.closeTime.IsZero() {
		t.Errorf("connection.closeTime is not zero after conection.Close() and seconf connection.Get() with expires option")
	}
	connection.Close()
}

func TestErrorHandler(t *testing.T) {
	var errorMarker error
	pool := New(&Options{
		ErrorHandler: func(err error) {
			errorMarker = err
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

	delete(pool.connections, path)

	connection.Close()
	time.Sleep(time.Second)
	if errorMarker == nil {
		t.Error("Error is not propagated to ErrorHandler")
	}

	connection, err = pool.Get(path)
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
