// Package netstorage pairs a netinsert client with a netselect client so the
// two together satisfy minilog.LogStorage.
//
// ---------------------------------------------------------------------------
// Reference: app/vlstorage/main.go, which holds exactly these two values
// (netstorageInsert, netstorageSelect) plus a localStorage, and dispatches to
// whichever is non-nil.
// ---------------------------------------------------------------------------
//
// There is almost nothing here, and that is the observation worth keeping. A
// node that inserts and a node that selects share no state, no coordination
// and no code -- they only share an address list. Everything that would
// normally be hard about running them together (consistency between what was
// written and what is readable, ordering, cross-component transactions) is
// absent because the storage nodes are the only stateful thing in the system
// and each one is a plain single-node minilog.
//
// If you ever add a field to this struct that both halves read, stop and
// re-read that paragraph. You would be introducing the first piece of shared
// distributed state in the design.
package netstorage

import (
	"github.com/niladrix719/minilog/minilog"
	"github.com/niladrix719/minilog/netinsert"
	"github.com/niladrix719/minilog/netselect"
)

// Config configures a Storage.
type Config struct {
	// Addrs are the storage node addresses, used by both halves.
	Addrs []string

	// Insert configures the insert half. Addrs is filled in from Addrs above.
	Insert netinsert.Config

	// Select configures the select half. Addrs is filled in from Addrs above.
	Select netselect.Config
}

// Storage is an insert+select node: the "vlinsert + vlselect" role.
type Storage struct {
	ins *netinsert.Storage
	sel *netselect.Storage
}

// NewStorage returns a Storage talking to cfg.Addrs.
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

// MustAddRows ingests rows via the insert half.
func (s *Storage) MustAddRows(rows []minilog.Row) {
	s.ins.MustAddRows(rows)
}

// Search queries every storage node via the select half.
func (s *Storage) Search(q *minilog.Query) ([]minilog.Row, *minilog.SearchStats) {
	return s.sel.Search(q)
}

// MustForceFlush drains client buffers, then makes every node's data
// queryable.
//
// The order is not arbitrary: netinsert.MustForceFlush must fully drain this
// process's send buffers to the nodes BEFORE anything asks the nodes to flush,
// or the flush races the rows it was meant to publish. netinsert's own
// MustForceFlush already does both steps in that order, so calling it alone is
// enough -- calling the select half's flush as well is harmless but redundant.
func (s *Storage) MustForceFlush() {
	s.ins.MustForceFlush()
}

// MustClose drains and closes both halves.
func (s *Storage) MustClose() {
	s.ins.MustClose()
	s.sel.MustClose()
}

// Insert exposes the insert half, for the stage 7 and 9 measurements that read
// its per-node counters.
func (s *Storage) Insert() *netinsert.Storage { return s.ins }

// The cluster facade is a LogStorage, exactly like the local engine. That
// assertion is the whole point of the seam: everything above this line can be
// written once.
var _ minilog.LogStorage = (*Storage)(nil)
