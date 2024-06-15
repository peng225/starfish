package agent

type PersistentStore interface {
	CurrentTerm() int64
	PutCurrentTerm(int64)
	VotedFor() int32
	PutVotedFor(int32)
	LogEntry(int64) *LogEntry
	LogSize() int64
	AppendLog(*LogEntry)
	CutOffLogTail(int64)
}
