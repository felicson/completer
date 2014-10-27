package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/tchap/go-patricia/patricia"
	"log"
	"net"
	"net/http"
	"os"
	//"pid"
	"runtime"
	"sort"
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
	db *sql.DB

	SKIPPED_WORDS          = map[string]bool{"и": true, "для": true}
	mysql_socket, mysql_db string
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

		if unicode.IsUpper(i) {
			i = unicode.ToLower(i)
		}

		switch {
		case unicode.IsLetter(i):
			if c > 0 && unicode.IsDigit(rune(raw[c-1])) {
				//разделяем цифры и буквы пробелом
				out.Write([]byte(" "))
			}
			l = utf8.RuneLen(i)

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
	return out.String()
}

func db_query(ids string) []byte {

	rows, err := db.Query(fmt.Sprintf("SELECT name,cpu FROM rubrics WHERE id IN(%s) ORDER BY FIELD(id,%s)", ids, ids))

	if err != nil {
		dbOpen()
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

type ranker struct {
	rubId  int
	weight int
}

type rankerList []*ranker

func (r *rankerList) Len() int {

	return len(*r)
}

func (r rankerList) Swap(i, j int) {

	r[j], r[i] = r[i], r[j]
}

func (r rankerList) Less(i, j int) bool {

	return r[i].weight > r[j].weight
}

func (r *rankerList) String() string {

	var str bytes.Buffer

	length := len(*r) - 1

	for i, rr := range *r {

		str.WriteString(strconv.Itoa(rr.rubId))

		if i > 10 {
			break
		}

		if i < length {
			str.WriteString(",")
		}
	}

	if len(*r) > 0 {

		return str.String()
	}
	return ""
}

func sphinx2(query string) []byte {

	query = query_prepare(query)

	buf := new(rankerList)

	ccache := make(map[int]*ranker, 1)


	for _, word := range strings.Fields(query) {

		err := trie.VisitSubtree(patricia.Prefix(word), func(prefix patricia.Prefix, item patricia.Item) error {

			rubId := item.(int)

			if v, ok := ccache[rubId]; ok {
				v.weight = v.weight + 1
			} else {
				rank := &ranker{rubId, 1}
				ccache[rubId] = rank
				*buf = append(*buf, rank)
			}

			return nil
		})

		if err != nil {
			panic(err)
		}
	}

	sort.Sort(buf)

	_ = (*buf).String()

	//if ids != "" {
	//	return db_query(ids)
	//}

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

func dbOpen() error {

	var err error
	db, err = sql.Open("mysql", fmt.Sprintf("myprom:weronica@unix(%s)/%s?parseTime=true", mysql_socket, mysql_db))
	//fmt.Printf("+++++ %v\n",err)
	return err

}

func init() {

	flag.StringVar(&mysql_socket, "mysql-socket", "/tmp/mysql.sock", "mysql-socket=/sock/path.sock")
	flag.StringVar(&mysql_db, "mysql-db", "myprom", "mysql-db=mydb")

	trie = patricia.NewTrie()

}

func main() {

	flag.Parse()

	runtime.GOMAXPROCS(2)

	cache = make(t_cache, 1)

	err := dbOpen()
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT name,id FROM rubrics WHERE counter > 0")

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

		for _, word := range strings.Fields(name) {

			word = fmt.Sprintf("%s_%d", word, id)

			trie.Insert(patricia.Prefix(word), id)
		}
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/completer/", completer)

	go cache_cleaner()

	//pid.CreatePid()
	err = Run(mux)
	if err != nil {
		panic(err)
	}

}
