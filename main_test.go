package main

import (
	//	"fmt"
	"testing"
)

func BenchmarkSimple(b *testing.B) {

	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = query_prepare("Труба профильная")
	}
}

func BenchmarkPatricia(b *testing.B) {
	b.StopTimer()
	err := dbOpen()
	if err != nil {
		panic(err)
	}

	initTrie()

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_ = sphinx2("Труба профильная")
	}
}
