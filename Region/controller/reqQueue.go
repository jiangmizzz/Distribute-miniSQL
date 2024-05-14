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

// IsHandled 返回该 req 是否已经成功处理过（不用再处理），用于去重
func (q *Queue) IsHandled(id string) bool {
	if !q.isExisted(id) {
		q.add(id)
		return false
	}
	return true
}

// add new reqId
func (q *Queue) add(val string) {
	if len(q.queue) == q.capacity { //满了
		removedID := q.queue[0]
		q.queue = q.queue[1:] //早的id先出队
		delete(q.hashSet, removedID)
	}
	q.queue = append(q.queue, val)
	q.hashSet[val] = true
}

// 检查reqId是否已经存在队列中
func (q *Queue) isExisted(id string) bool {
	return q.hashSet[id] //不存在时，返回值类型的零值 (false)
}
