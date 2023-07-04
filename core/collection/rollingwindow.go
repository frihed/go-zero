package collection

import (
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/timex"
)

type (
	// RollingWindowOption let callers customize the RollingWindow.
	RollingWindowOption func(rollingWindow *RollingWindow)

	// RollingWindow defines a rolling window to calculate the events in buckets with time interval.
	// 这个滑动窗口实现很不错，学习如何处理连续信号！
	RollingWindow struct {
		lock          sync.RWMutex
		// 窗口分片数量，即窗口内的采样数 
		size          int
		// 实际窗口，数据容器
		win           *window
		// 采样间隔
		interval      time.Duration
		// 游标位置
		offset        int
		ignoreCurrent bool
		// 最后一个时间片的起始点（可以理解为采样点，离散信号）
		lastTime      time.Duration // start time of the last bucket
	}
	// 40个采样点，间隔 250ms，则滑动窗口总大小 10s（10s的连续信号，分为40个区间采样 —— 由40个离散信号来统计这个连续区间）
)

// NewRollingWindow returns a RollingWindow that with size buckets and time interval,
// use opts to customize the RollingWindow.
func NewRollingWindow(size int, interval time.Duration, opts ...RollingWindowOption) *RollingWindow {
	if size < 1 {
		panic("size must be greater than 0")
	}

	w := &RollingWindow{
		size:     size,
		win:      newWindow(size),
		interval: interval,
		lastTime: timex.Now(),
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Add adds value to current bucket.
func (rw *RollingWindow) Add(v float64) {
	rw.lock.Lock()
	defer rw.lock.Unlock()
	// 调整游标
	rw.updateOffset()
	// 统计记录
	rw.win.add(rw.offset, v)
}

// Reduce runs fn on all buckets, ignore current bucket if ignoreCurrent was set.
// 在滑动窗口有效区间上执行统计的函数
func (rw *RollingWindow) Reduce(fn func(b *Bucket)) {
	rw.lock.RLock()
	defer rw.lock.RUnlock()

	// diff 为窗口内，有效采样数量
	var diff int 
	// 当前时间与上一个采样点的跨度
	span := rw.span()
	// ignore current bucket, because of partial data
	if span == 0 && rw.ignoreCurrent {
		// span 为零，依然在当前采样时间段内
		// ignoreCurrent 可能当前时间段的统计会有偏差，所以在整个窗口上，排除当前小窗口，即为需要统计的范围
		diff = rw.size - 1
	} else {
		// 当 span 不为 0， 说明有一些小窗口数据无效、已过期
		// offset 指向窗口完整且有效，当前时间窗口为可覆盖的历史窗口，无效
		diff = rw.size - span
	}
	if diff > 0 {
		//rw.offset + span 为过期窗口，无效数据，从 +1 开始（当前时间点也是无效数据）
		offset := (rw.offset + span + 1) % rw.size
		// 从 offset 开始，遍历 diff 个，注意理解这里，如何用离散信号来处理连续信号
		// 这里遍历的是落在整个时间窗口内的有效时间段
		// 并不是绝对意义的 (current-W, current), 处理连续信号要记录每一个数据以及时间点，是很难处理的
		rw.win.reduce(offset, diff, fn) // 从有效采样点开始，遍历 diff 个
	}
}

// 当前时间与上一个采样点的跨度
func (rw *RollingWindow) span() int {
	// 看这里，这里有取整操作
	// 时间是连续的，属于模拟信号，有无限取值，这里细分为小时间段，来离散化
	// 将当前时间落在了某个小片段，将模拟信号变为不连续的、离散的数字信号！！！！！
	// 微积分思想在处理连续信号时，非常重要！！
	offset := int(timex.Since(rw.lastTime) / rw.interval)
	if 0 <= offset && offset < rw.size {
		return offset
	}
	// 跨度以及超过完整的窗口，记为窗口大小即可	
	return rw.size
}

// 这个函数实现了时间窗口的滑动
func (rw *RollingWindow) updateOffset() {
	span := rw.span()
	if span <= 0 {
		return
	}

	offset := rw.offset
	// reset expired buckets
	for i := 0; i < span; i++ {
		rw.win.resetBucket((offset + i + 1) % rw.size)
	}

	rw.offset = (offset + span) % rw.size
	now := timex.Now()
	// align to interval time boundary
	// 这里很重要，lastTime 记录的并不是事件发生时间点，而是当前时间片的起点值，可以理解为连续信号的采样点
	rw.lastTime = now - (now-rw.lastTime)%rw.interval
}

// Bucket defines the bucket that holds sum and num of additions.
type Bucket struct {
	Sum   float64
	Count int64
}

func (b *Bucket) add(v float64) {
	b.Sum += v
	b.Count++
}

func (b *Bucket) reset() {
	b.Sum = 0
	b.Count = 0
}

// 窗口，即数据容器
type window struct {
	buckets []*Bucket
	size    int
}

func newWindow(size int) *window {
	buckets := make([]*Bucket, size)
	// Bucket 初始化
	for i := 0; i < size; i++ {
		buckets[i] = new(Bucket)
	}
	return &window{
		buckets: buckets,
		size:    size,
	}
}

func (w *window) add(offset int, v float64) {
	w.buckets[offset%w.size].add(v)
}

func (w *window) reduce(start, count int, fn func(b *Bucket)) {
	for i := 0; i < count; i++ {
		fn(w.buckets[(start+i)%w.size])
	}
}

func (w *window) resetBucket(offset int) {
	w.buckets[offset%w.size].reset()
}

// IgnoreCurrentBucket lets the Reduce call ignore current bucket.
func IgnoreCurrentBucket() RollingWindowOption {
	return func(w *RollingWindow) {
		w.ignoreCurrent = true
	}
}
