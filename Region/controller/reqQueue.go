package controller

type Queue struct {
	queue    []string        //队列
	capacity int             //容量
	hashSet  map[string]bool //便于遍历
}

const capacity = 100

var reqQueue = Queue{
	queue:    make([]string, 0, capacity),
	capacity: capacity,
	hashSet:  make(map[string]bool),
}

// Add add new reqId
func (q *Queue) Add(val string) {
	if len(q.queue) == q.capacity { //满了
		removedID := q.queue[0]
		q.queue = q.queue[1:] //早的id先出队
		delete(q.hashSet, removedID)
	}
	q.queue = append(q.queue, val)
	q.hashSet[val] = true
}

// IsExisted 检查reqId是否已经存在队列中
func (q *Queue) IsExisted(id string) bool {
	return q.hashSet[id] //不存在时，返回值类型的零值 (false)
}
