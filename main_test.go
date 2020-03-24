package main

import (
	"myprom/completer/storage"
	//	"fmt"
	"testing"
)

func BenchmarkSimple(b *testing.B) {

	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = queryPrepare("Труба профильная")
	}
}

func BenchmarkPatricia(b *testing.B) {
	b.StopTimer()
	storage, err := storage.NewStorage("test", "test", "test")
	if err != nil {
		panic(err)
	}

	initTrie(storage)

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		search("Труба профильная")
	}
}
