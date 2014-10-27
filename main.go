package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tchap/go-patricia/patricia"
	"log"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	SOCKET = "/tmp/completer.sock"
)

var (
	db, sph_db *sql.DB

	SKIPPED_WORDS = map[string]bool{"и": true, "для": true}
)

type Complete struct {
	Name string `json:"name"`
	Cpu  string `json:"cpu"`
}

type Cacher struct {
	key   string
	ctime time.Time
}

type t_cache map[string]*Cacher

var cache t_cache

var trie *patricia.Trie

func query_prepare(raw string) string {

	var out bytes.Buffer
	var l int
	for c, i := range raw {

		switch {
		case unicode.IsLetter(i):
			if c > 0 && unicode.IsDigit(rune(raw[c-1])) {
				//разделяем цифры и буквы пробелом
				out.Write([]byte(" "))
			}
			l = utf8.RuneLen(i)

			if unicode.IsUpper(i) {
				i = unicode.ToLower(i)
			}
			out.WriteString(string(i))
			continue

		case unicode.IsDigit(i):

			if c > 0 && unicode.IsLetter(rune(raw[c-l])) {
				out.Write([]byte(" "))
			}

			out.WriteString(string(i))
			continue

		default:

			out.Write([]byte(" "))
		}
	}
	str := out.Bytes()

	if !bytes.HasSuffix(str, []byte("* ")) {

		//out.Write([]byte("*"))
	}
	return out.String()
}

func db_query(ids string) []byte {

	rows, err := db.Query(fmt.Sprintf("SELECT name,cpu FROM rubrics WHERE id IN(%s)", ids))

	if err != nil {
		panic(err)
	}

	defer rows.Close()

	rubrics := make([]Complete, 0)

	for rows.Next() {

		var name, cpu string

		err = rows.Scan(&name, &cpu)

		if err != nil {
			panic(err)
		}
		rubrics = append(rubrics, Complete{name, cpu})

	}

	b, err := json.Marshal(rubrics)
	return b

}

func sphinx(query string) []byte {

	var buf []string
	var ids string

	query = query_prepare(query)

	//fmt.Println(query)
	if val, ok := cache[query]; ok {

		ids = val.key

	} else {

		rows, err := sph_db.Query(fmt.Sprintf("SELECT sphinx_internal_id FROM rubric WHERE MATCH('%s') AND counter > 0 LIMIT 25 OPTION max_matches=40;", query))

		if err != nil {
			panic(err)
		}

		defer rows.Close()

		var id int

		for rows.Next() {

			err = rows.Scan(&id)

			if err != nil {
				panic(err)
			}
			buf = append(buf, strconv.Itoa(id))

		}
		ids = strings.Join(buf, ",")

		cache[query] = &Cacher{ids, time.Now()}
	}

	if ids != "" {
		return db_query(ids)
	}
	return []byte("[]")

}

func sphinx2(query string) []byte {

	query = query_prepare(query)

	var buf []string
	var ids string

	err := trie.VisitSubtree(patricia.Prefix(query), func(prefix patricia.Prefix, item patricia.Item) error {

		buf = append(buf, strconv.Itoa(item.(int)))
		return nil
	})

	if err != nil {
		panic(err)
	}

	ids = strings.Join(buf, ",")

	if ids != "" {
		return db_query(ids)
	}

	return []byte("[]")
}

func completer(w http.ResponseWriter, req *http.Request) {

	query := req.FormValue("query")

	w.Header().Set("Content-type", "application/json; charset=utf-8")

	if query != "" {

		w.Write(sphinx2(query))
		return
	}
	w.Write([]byte("[]"))

}

func Run(mux *http.ServeMux) error {

	if _, err := os.Stat(SOCKET); err == nil {

		log.Print("Remove old socket")
		os.Remove(SOCKET)
	}

	addr, err := net.ResolveUnixAddr("unix", SOCKET)

	if err != nil {

		return err
	}

	ln, err := net.ListenUnix("unix", addr)

	err = os.Chmod(SOCKET, 0777)

	if err != nil {
		return err
	}

	log.Print("Change mode for socket")

	log.Print("Run server!!!")

	err = http.Serve(ln, mux)

	if err != nil {
		return err
	}

	return nil

}

func cache_cleaner() {

	c := time.Tick(24 * time.Hour)

	for _ = range c {
		cache = make(t_cache, 1)
	}
}

func init() {

	var err error
	sph_db, err = sql.Open("mysql", "tcp(127.0.0.1:9308)/rubric")

	if err != nil {
		panic(err)
	}

	db, err = sql.Open("mysql", "myprom:weronica@unix(/var/run/mysqld/mysqld.sock)/myprom_ror?parseTime=true")

	if err != nil {
		panic(err)
	}

	trie = patricia.NewTrie()

	rows, err := db.Query("SELECT name,id FROM rubrics")

	if err != nil {
		panic(err)
	}

	defer rows.Close()

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

		trie.Insert(patricia.Prefix(name), id)
	}
}

func main() {

	runtime.GOMAXPROCS(2)

	cache = make(t_cache, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/completer/", completer)

	go cache_cleaner()

	err := Run(mux)
	if err != nil {
		panic(err)
	}

}
