package main

import (
	"fmt"
	"strings"
//	"fmt"
	"testing"
	"github.com/tchap/go-patricia/patricia"
)


func BenchmarkSimple(b *testing.B){

	for n := 0; n < b.N; n++ {
		_ = query_prepare("Труба профильная")
	}
}


func BenchmarkPatricia(b *testing.B){
	b.StopTimer()
	err := dbOpen()
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT name,id FROM rubrics WHERE counter > 0")

	if err != nil {
		panic(err)
	}

	defer rows.Close()

	trie = patricia.NewTrie()

	for rows.Next() {

		var (
			name string
			id   int
		)

		err = rows.Scan(&name, &id)

		if err != nil {
			panic(err)
		}
		name = query_prepare(name)

		for _, word := range strings.Fields(name) {

			word = fmt.Sprintf("%s_%d", word, id)

			trie.Insert(patricia.Prefix(word), id)
		}
	}

	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_ = sphinx2("Труба профильная")
	}
}


