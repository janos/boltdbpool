// Copyright (c) 2015, 2016 Janoš Guljaš <janos@resenje.org>
// All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package timed

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUnknownPeriod(t *testing.T) {
	dir, err := ioutil.TempDir("", "boltdbpool-timed")
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()
	pool, err := New(dir, 0, nil)
	if err != ErrUnknownPeriod {
		t.Errorf("expected error %v, got %v", ErrUnknownPeriod, err)
	}
	if pool != nil {
		t.Errorf("expected nil Pool, got %#v", pool)
	}
}

func TestHourlyPeriod(t *testing.T) {
	dir, err := ioutil.TempDir("", "boltdbpool-timed")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

	currTime := time.Now()
	nextTime := currTime.Add(time.Hour)
	prevTime := currTime.Add(-time.Hour)

	setupPool, err := New(dir, Hourly, nil)
	if err != nil {
		t.Error(err)
	}
	if setupPool == nil {
		t.Error("got nil pool")
	}
	_, err = setupPool.NewConnection(currTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(nextTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(prevTime)
	if err != nil {
		t.Error(err)
	}
	setupPool.Close()

	pool, err := New(dir, Hourly, nil)
	if err != nil {
		t.Error(err)
	}
	if pool == nil {
		t.Error("got nil pool")
	}
	currConn, err := pool.GetConnection(currTime)
	if err != nil {
		t.Error(err)
	}

	t.Run("ConnectionCurrentTime", func(t *testing.T) {
		series := currTime.Format("2006010215")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionNextTime", func(t *testing.T) {
		if _, err = currConn.Next(); err != nil {
			t.Error(err)
		}
		series := nextTime.Format("2006010215")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionPrevTime", func(t *testing.T) {
		if _, err = currConn.Prev(); err != nil {
			t.Error(err)
		}
		series := prevTime.Format("2006010215")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolGetConnection", func(t *testing.T) {
		if _, err := pool.GetConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextConnection", func(t *testing.T) {
		if _, err := pool.NextConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextNextConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(10 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.NextConnection(time.Now().Add(5 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrepvPrevConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(-10 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.PrevConnection(time.Now().Add(-5 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrevConnection", func(t *testing.T) {
		if _, err := pool.PrevConnection(currTime); err != nil {
			t.Error(err)
		}
	})
}

func TestDailyPeriod(t *testing.T) {
	dir, err := ioutil.TempDir("", "boltdbpool-timed")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

	currTime := time.Now()
	nextTime := currTime.Add(24 * time.Hour)
	prevTime := currTime.Add(-24 * time.Hour)

	setupPool, err := New(dir, Daily, nil)
	if err != nil {
		t.Error(err)
	}
	if setupPool == nil {
		t.Error("got nil pool")
	}
	_, err = setupPool.NewConnection(currTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(nextTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(prevTime)
	if err != nil {
		t.Error(err)
	}
	setupPool.Close()

	pool, err := New(dir, Daily, nil)
	if err != nil {
		t.Error(err)
	}
	if pool == nil {
		t.Error("got nil pool")
	}
	currConn, err := pool.GetConnection(currTime)
	if err != nil {
		t.Error(err)
	}

	t.Run("ConnectionCurrentTime", func(t *testing.T) {
		series := currTime.Format("20060102")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionNextTime", func(t *testing.T) {
		if _, err = currConn.Next(); err != nil {
			t.Error(err)
		}
		series := nextTime.Format("20060102")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionPrevTime", func(t *testing.T) {
		if _, err = currConn.Prev(); err != nil {
			t.Error(err)
		}
		series := prevTime.Format("20060102")
		if _, err := os.Stat(filepath.Join(dir, series[:6], series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolGetConnection", func(t *testing.T) {
		if _, err := pool.GetConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextConnection", func(t *testing.T) {
		if _, err := pool.NextConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextNextConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(10 * 24 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.NextConnection(time.Now().Add(5 * 24 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrepvPrevConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(-10 * 24 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.PrevConnection(time.Now().Add(-5 * 24 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrevConnection", func(t *testing.T) {
		if _, err := pool.PrevConnection(currTime); err != nil {
			t.Error(err)
		}
	})
}

func TestMonthlyPeriod(t *testing.T) {
	dir, err := ioutil.TempDir("", "boltdbpool-timed")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

	currTime := time.Now()
	nextTime := currTime.Add(24 * 31 * time.Hour)
	prevTime := currTime.Add(-24 * 31 * time.Hour)

	setupPool, err := New(dir, Monthly, nil)
	if err != nil {
		t.Error(err)
	}
	if setupPool == nil {
		t.Error("got nil pool")
	}
	_, err = setupPool.NewConnection(currTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(nextTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(prevTime)
	if err != nil {
		t.Error(err)
	}
	setupPool.Close()

	pool, err := New(dir, Monthly, nil)
	if err != nil {
		t.Error(err)
	}
	if pool == nil {
		t.Error("got nil pool")
	}
	currConn, err := pool.GetConnection(currTime)
	if err != nil {
		t.Error(err)
	}

	t.Run("ConnectionCurrentTime", func(t *testing.T) {
		series := currTime.Format("200601")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionNextTime", func(t *testing.T) {
		if _, err = currConn.Next(); err != nil {
			t.Error(err)
		}
		series := nextTime.Format("200601")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionPrevTime", func(t *testing.T) {
		if _, err = currConn.Prev(); err != nil {
			t.Error(err)
		}
		series := prevTime.Format("200601")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolGetConnection", func(t *testing.T) {
		if _, err := pool.GetConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextConnection", func(t *testing.T) {
		if _, err := pool.NextConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextNextConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(10 * 24 * 31 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.NextConnection(time.Now().Add(5 * 24 * 31 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrepvPrevConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(-10 * 24 * 31 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.PrevConnection(time.Now().Add(-5 * 24 * 31 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrevConnection", func(t *testing.T) {
		if _, err := pool.PrevConnection(currTime); err != nil {
			t.Error(err)
		}
	})
}

func TestYearlyPeriod(t *testing.T) {
	dir, err := ioutil.TempDir("", "boltdbpool-timed")
	if err != nil {
		t.Error(err)
	}
	defer func() {
		if err = os.RemoveAll(dir); err != nil {
			t.Error(err)
		}
	}()

	currTime := time.Now()
	nextTime := currTime.Add(24 * 366 * time.Hour)
	prevTime := currTime.Add(-24 * 366 * time.Hour)

	setupPool, err := New(dir, Yearly, nil)
	if err != nil {
		t.Error(err)
	}
	if setupPool == nil {
		t.Error("got nil pool")
	}
	_, err = setupPool.NewConnection(currTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(nextTime)
	if err != nil {
		t.Error(err)
	}
	_, err = setupPool.NewConnection(prevTime)
	if err != nil {
		t.Error(err)
	}
	setupPool.Close()

	pool, err := New(dir, Yearly, nil)
	if err != nil {
		t.Error(err)
	}
	if pool == nil {
		t.Error("got nil pool")
	}
	currConn, err := pool.GetConnection(currTime)
	if err != nil {
		t.Error(err)
	}

	t.Run("ConnectionCurrentTime", func(t *testing.T) {
		series := currTime.Format("2006")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionNextTime", func(t *testing.T) {
		if _, err = currConn.Next(); err != nil {
			t.Error(err)
		}
		series := nextTime.Format("2006")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("ConnectionPrevTime", func(t *testing.T) {
		if _, err = currConn.Prev(); err != nil {
			t.Error(err)
		}
		series := prevTime.Format("2006")
		if _, err := os.Stat(filepath.Join(dir, series+".db")); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolGetConnection", func(t *testing.T) {
		if _, err := pool.GetConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextConnection", func(t *testing.T) {
		if _, err := pool.NextConnection(currTime); err != nil {
			t.Error(err)
		}
	})

	t.Run("PoolNextNextConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(10 * 24 * 366 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.NextConnection(time.Now().Add(5 * 24 * 366 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrepvPrevConnection", func(t *testing.T) {
		c, err := pool.NewConnection(time.Now().Add(-10 * 24 * 366 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		c2, err := pool.PrevConnection(time.Now().Add(-5 * 24 * 366 * time.Hour))
		if err != nil {
			t.Error(err)
		}
		if c.series != c2.series {
			t.Errorf("expected two connection series to be the same: %s, %s", c.series, c2.series)
		}
	})

	t.Run("PoolPrevConnection", func(t *testing.T) {
		if _, err := pool.PrevConnection(currTime); err != nil {
			t.Error(err)
		}
	})
}
