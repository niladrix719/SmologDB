package agent

// ---------------------------------------------------------------------------
// STAGE 12 -- YOUR IMPLEMENTATION GOES HERE.
//
// Verified by:  go test ./agent -run TestStage12 -v     (readNewLines, checkpoints)
//               go test ./harness -run TestStage12Tailer -v
//
// Reference: app/vlagent/tail/{tailer.go,logfile.go,checkpoints_db.go} and
// app/vlagent/filecollector/. The real one uses inotify-style notification,
// fingerprints files to survive rotation, and tracks many files at once. This
// one polls, tracks an offset per path, and survives truncation.
// ---------------------------------------------------------------------------

import (
	"time"

	"github.com/niladrix719/minilog/minilog"
)

// The tailer is the agent's second input: it follows log files that
// applications already write, the way `tail -F` does, and turns each new line
// into a Row.
//
//	app writes app.log  ->  tailer reads new lines  ->  agent.MustAddRows
//	                                    |
//	                         checkpoints.json: how far we have read
//
// Everything interesting is in the last line of that diagram. The tailer has
// state that outlives it -- "I have read this file up to byte N" -- and that
// state has the same problem the queue had: it must be correct after a crash
// at ANY point.

// DefaultPollInterval is how often the tailer looks for new data.
//
// Polling, not inotify/kqueue: it is portable, it is twenty lines, and 250ms
// of latency is invisible next to the agent's own 1s flush interval. The real
// one is notification-driven and cheaper at scale; that is an upgrade, not a
// correction.
const DefaultPollInterval = 250 * time.Millisecond

// maxReadPerPoll bounds how much of one file readNewLines returns per call.
// A tailer started on a 20GB log must not try to hold 20GB of lines in memory
// before it sends the first one; it reads this much, delivers it, checkpoints,
// and comes back for the next piece.
const maxReadPerPoll = 4 << 20

// TailerConfig configures a Tailer.
type TailerConfig struct {
	// Paths are the files to follow. No globbing; a path that does not exist
	// yet is not an error, it is a file that has not been created. Check for
	// it again on the next poll.
	Paths []string

	// CheckpointPath is a JSON file mapping each path to the offset the
	// tailer has read up to and handed to the sink durably:
	//
	//	{"/var/log/app.log": 1048576, "/var/log/other.log": 312}
	//
	// Missing or unparseable means "no checkpoints": start every file from
	// offset 0. Log it, do not panic -- a corrupt checkpoint file must not
	// make the agent unstartable.
	CheckpointPath string

	// PollInterval 0 means DefaultPollInterval.
	PollInterval time.Duration
}

// readNewLines returns the COMPLETE lines in path from offset onward, and the
// offset just past the last complete line it returned.
//
// "Complete" is the point. A writer appends "hello wor" and then, a moment
// later, "ld\n". If you read between the two and take "hello wor" as a line,
// you ingest a fragment and, worse, advance the offset past it so the other
// half arrives as a second fragment. A line is not a line until its '\n' has
// been written, so bytes after the last '\n' are NOT consumed: newOffset stops
// at the byte after the last '\n', and the next call starts at the beginning of
// the unfinished line.
//
// The other cases:
//
//   - path does not exist: no lines, newOffset == offset, err == nil.
//   - the file is SMALLER than offset: it was truncated or replaced (log
//     rotation by copytruncate does exactly this). Start again from 0.
//     Without this the tailer waits forever at an offset the file will never
//     reach again, and the log goes silent with no error anywhere. (Limit,
//     worth knowing: if the file is truncated and then grows PAST the old
//     offset before the next poll, size alone cannot see it. The real one
//     fingerprints the first bytes of the file for this reason.)
//   - lines end in "\n" or "\r\n"; strip the "\r" too.
//   - a BLANK line produces no entry, but the offset still advances over it.
//   - read at most maxReadPerPoll bytes; return what is complete within them.
//     A single line longer than that with no newline in sight: split it at
//     the limit rather than buffering without bound, and say so in the log.
//
// The returned slices must not alias a buffer that the next call reuses.
func readNewLines(path string, offset int64) (lines [][]byte, newOffset int64, err error) {
	panic("TODO stage 12: implement readNewLines")
}

// lineToRow turns one text line into a Row. COMPLETE.
//
// The message is the line, the stream is the file, and the time is when we
// read it. The row's fields are sorted by name, as Row requires: "_msg" sorts
// before "file" because '_' (0x5F) is below 'f' (0x66).
//
// Extension, not tested: a line that starts with '{' could go through
// ParseJSONLine instead, which is what the real collector does for JSON logs.
func lineToRow(path string, line []byte, now int64) minilog.Row {
	labels := []minilog.Field{{Name: "file", Value: path}}
	return minilog.Row{
		Timestamp: now,
		StreamID:  minilog.StreamIDForLabels(labels),
		Fields: []minilog.Field{
			{Name: "_msg", Value: string(line)},
			{Name: "file", Value: path},
		},
	}
}

// mustWriteFileAtomic replaces path with data so that a crash at any moment
// leaves either the complete old contents or the complete new contents, never
// a mixture and never an empty file.
//
// The sequence you already wrote twice -- mustWritePartsJSON in stage 3 and
// queue.mustWriteMetainfo in stage 11:
//
//  1. write to path+".tmp"
//  2. fsync the temp file
//  3. rename it over path        (atomic on POSIX)
//  4. fsync the directory        (so the rename itself is durable)
//
// This is the third copy of that sequence. Write it as a function this time,
// and consider pointing queue.mustWriteMetainfo at it: three copies of the one
// thing that is easy to get subtly wrong is two too many.
func mustWriteFileAtomic(path string, data []byte) {
	panic("TODO stage 12: implement mustWriteFileAtomic")
}

// loadCheckpoints reads the checkpoint file. It never fails: a missing,
// unreadable or corrupt file yields an empty, NON-NIL map (so callers can
// assign into it) and, for the corrupt case, a log line.
func loadCheckpoints(path string) map[string]int64 {
	panic("TODO stage 12: implement loadCheckpoints")
}

// mustSaveCheckpoints writes the map as JSON through mustWriteFileAtomic.
func mustSaveCheckpoints(path string, cps map[string]int64) {
	panic("TODO stage 12: implement mustSaveCheckpoints")
}

// Tailer follows files and feeds a sink.
//
// You will need: the config, the sink, a map of path -> offset (the in-memory
// progress), a stop channel, and a WaitGroup for the poll goroutine.
type Tailer struct {
	// TODO stage 12
}

// NewTailer loads the checkpoints and starts the poll goroutine.
//
// A path with no checkpoint starts at offset 0: the existing contents of the
// file are ingested, not skipped. (`tail -f` would start at the end. A log
// COLLECTOR must not: the lines written before it started are logs too.)
func NewTailer(cfg *TailerConfig, sink FlushSink) *Tailer {
	panic("TODO stage 12: implement NewTailer")
}

// pollOnce reads whatever is new in every file, delivers it, and checkpoints.
//
// THE ORDER IS THE WHOLE POINT:
//
//  1. readNewLines for each path, turn the lines into rows with lineToRow,
//     and call sink.MustAddRows.
//  2. sink.MustFlush() -- the rows are now ON DISK in the agent's queue.
//  3. ONLY THEN save the checkpoints with the new offsets.
//
// Do it the other way round and a crash between "checkpoint saved" and "rows
// on disk" loses those lines: the restarted tailer believes they were
// delivered. Do it in this order and the worst a crash can do is leave the
// checkpoint one step behind, so the lines are read again and delivered
// twice. That is at-least-once, the same bargain as the queue's peek/ack, and
// for the same reason: the cheap failure is a duplicate, the expensive one is
// a hole.
//
// Skip steps 2 and 3 when nothing new was read; an idle tailer should not
// fsync once a poll.
//
// The in-memory offsets advance as soon as lines are read (so the next poll
// does not read them again); the FILE only changes in step 3.
func (t *Tailer) pollOnce() {
	panic("TODO stage 12: implement Tailer.pollOnce")
}

// MustClose stops polling, does one final pollOnce so lines written just
// before shutdown are not left for the next run, and returns. The sink is the
// caller's: the tailer does not close it.
func (t *Tailer) MustClose() {
	panic("TODO stage 12: implement Tailer.MustClose")
}
