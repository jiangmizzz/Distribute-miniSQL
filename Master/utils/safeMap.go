package utils

import "sync"

type SafeMap[K comparable, V any] struct {
	Mu  sync.RWMutex
	Map map[K]V
}
