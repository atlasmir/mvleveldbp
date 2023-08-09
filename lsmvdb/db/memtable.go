package db

import (
	"github.com/MauriceGit/skiplist"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
)

type Memtable struct {
	creationTime ValidTime
	archiveTime  ValidTime
	//data  map[Key]*skiplist.SkipList
	// data is a red-black tree of *skiplist.SkipList
	data *rbt.Tree
}

func NewMemtable(creationTime ValidTime) *Memtable {

	//data := make(map[Key]*skiplist.SkipList)
	data := rbt.NewWith(KeyComparator)
	return &Memtable{
		creationTime: creationTime,
		data:         data,
	}
}

func (mem *Memtable) Archive(time ValidTime) {
	mem.archiveTime = time
}

func (mem *Memtable) Put(key Key, sequenceNumber SequenceNumber, creationTime ValidTime, value string) (err error) {
	err = nil
	list, exist := mem.data.Get(key)
	if exist != true {
		newVersionList := skiplist.New()
		mem.data.Put(key, &newVersionList)
		list = &newVersionList
	}
	list.(*skiplist.SkipList).Insert(Message{sequenceNumber: sequenceNumber, creationTime: creationTime, value: value})

	return
}

// Get function returns the message body, the status and the sequence number of the next message to a query.
// the returned status
func (mem *Memtable) Get(key Key, time ValidTime) (message Message, status Status, nextSequence SequenceNumber, err error) {
	err = nil
	treeNodeValue, exist := mem.data.Get(key)
	if exist != true {
		return Message{}, Status(NOTFOUND), 0, nil
	}
	list := treeNodeValue.(*skiplist.SkipList)
	elem, ok := list.FindGreaterOrEqual(Message{creationTime: time})
	if ok {
		message := elem.GetValue().(Message)
		var next Message
		if time == message.creationTime {
			// correct version
			if elem == list.GetLargestNode() {
				// this is the last version
				return message, Status(ODV), 0, nil
			}
			next = list.Next(elem).GetValue().(Message)
		} else {
			if elem == list.GetSmallestNode() {
				// this is the first version, and it has not been generated at time.
				return Message{}, Status(NOTFOUND), message.sequenceNumber, nil
			}
			// go backward
			next = message
			message = list.Prev(elem).GetValue().(Message)
		}
		nextSequence = next.sequenceNumber
		// Check the sequence number of the successive version
		if nextSequence == message.sequenceNumber+1 {
			// non-ODV. status set to OK
			return message, Status(OK), nextSequence, nil
		} else if nextSequence > message.sequenceNumber+1 {
			// HOLE
			return message, Status(HOLE), nextSequence, nil
		} else {
			// nextSequence <= message.sequenceNumber
			return Message{}, Status(ERROR), nextSequence, SequenceOutOfOrder{message.sequenceNumber, nextSequence}
		}
	} else {
		// matches last version
		return list.GetLargestNode().GetValue().(Message), Status(ODV), 0, nil
	}
}
