package storage

type LogStorage interface {
	MustAddRows(rows []Row)

	Search(q *Query) ([]Row, *SearchStats)

	MustForceFlush()

	MustClose()
}

var _ LogStorage = (*Storage)(nil)
