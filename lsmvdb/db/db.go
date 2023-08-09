package db

import "sync"

type FileMetadata struct {
	id             int
	lowerValidTime ValidTime
	upperValidTime ValidTime
}

type DB struct {
	// file path
	path string

	// core data structures
	mu  sync.Mutex
	mem *Memtable
	imm *Memtable

	// component management
	files []FileMetadata

	// helper fields for experiments
	sensors map[int][]int // stores the sensor properties
}

func NewDB(path string, creationTime ValidTime) *DB {
	return &DB{
		path: path,
		mem:  NewMemtable(0),
	}
}

func (db *DB) nextID() int {
	return db.files[len(db.files)-1].id + 1
}

func (db *DB) Put(key Key, sequenceNumber SequenceNumber, creationTime ValidTime, value string) (err error) {
	// TODO: implement multiple components
	err = db.mem.Put(key, sequenceNumber, creationTime, value)
	return
}

func (db *DB) Get(key Key, time ValidTime) (message Message, status Status, nextSequence SequenceNumber, err error) {
	message, status, nextSequence, err = db.mem.Get(key, time)
	return
}

func (db *DB) SetSensors(sensors map[int][]int) {
	db.sensors = sensors
}

func (db *DB) CompactMemTable(time ValidTime) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.imm = db.mem
	db.imm.Archive(time)
	db.mem = NewMemtable(time)
}

func (db *DB) SaveComponent() {

}
