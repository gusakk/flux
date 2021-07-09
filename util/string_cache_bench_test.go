package intern

import (
	"strconv"
	"testing"
)

func BenchmarkAdd(b *testing.B) {
	var words []string
	for i := 0; i < 1024; i++ {
		words = append(words, strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// create instance of string cache.
		cache := NewStringCache(100000)
		b.StartTimer()
		for _, word := range words {
			cache.Add(word)
		}
	}
}

func BenchmarkAddWithEviction(b *testing.B) {
	var words []string
	for i := 1024; i < 2048; i++ {
		words = append(words, strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// create instance of string cache with low max cache size.
		cache := NewStringCache(1)
		b.StartTimer()
		for _, word := range words {
			cache.Add(word)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	var words []string
	for i := 1024; i < 2048; i++ {
		words = append(words, strconv.Itoa(i))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		// create instance of string cache.
		cache := NewStringCache(100000)
		for _, word := range words {
			cache.Add(word)
		}
		b.StartTimer()
		for _, word := range words {
			cache.Get(word)
		}
	}
}