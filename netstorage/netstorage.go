// Package netstorage pairs netinsert and netselect so a cluster looks like one storage.LogStorage.
package netstorage

import (
	"github.com/niladrix719/smologdb/netinsert"
	"github.com/niladrix719/smologdb/netselect"
	"github.com/niladrix719/smologdb/storage"
)

type Config struct {
	Addrs []string

	Insert netinsert.Config

	Select netselect.Config
}

type Storage struct {
	ins *netinsert.Storage
	sel *netselect.Storage
}

func NewStorage(cfg *Config) *Storage {
	insCfg := cfg.Insert
	insCfg.Addrs = cfg.Addrs
	selCfg := cfg.Select
	selCfg.Addrs = cfg.Addrs

	return &Storage{
		ins: netinsert.NewStorage(&insCfg),
		sel: netselect.NewStorage(&selCfg),
	}
}

func (s *Storage) MustAddRows(rows []storage.Row) {
	s.ins.MustAddRows(rows)
}

func (s *Storage) Search(q *storage.Query) ([]storage.Row, *storage.SearchStats) {
	return s.sel.Search(q)
}

func (s *Storage) MustForceFlush() {
	s.ins.MustForceFlush()
}

func (s *Storage) MustClose() {
	s.ins.MustClose()
	s.sel.MustClose()
}

func (s *Storage) Insert() *netinsert.Storage { return s.ins }

var _ storage.LogStorage = (*Storage)(nil)
