package main

import (
	"bytes"
	"database/sql"
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
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	SOCKET       = "/tmp/completer.sock"
	DISPLAYLIMIT = 10
)

var (
	db *sql.DB

	SKIPPED_WORDS          = map[string]bool{"и": true, "для": true, "к": true}
	mysql_socket, mysql_db string

	ccachePool sync.Pool
	uniqPool   sync.Pool
	tmpPool    sync.Pool
	outPool    sync.Pool
)

type rubric struct {
	Name string `json:"name"`
	Cpu  string `json:"cpu"`
}

type Complete struct {
	rubId  int
	repeat uint8
}

var rubrics map[int]rubric

var trie *patricia.Trie //main data structure

type ranker struct {
	rubId     int
	weight    uint8
	uniqWords uint8
	Item      *Complete
}

type rankerList []*ranker

func isSkipped(word []byte) bool {

	if _, ok := SKIPPED_WORDS[string(word)]; ok {

		return ok
	}
	return false
}
func query_prepare(raw string) string {

	//var tmp bytes.Buffer
	out := outPool.Get().(*bytes.Buffer)
	tmp := tmpPool.Get().(*bytes.Buffer)
	defer out.Reset()
	defer tmp.Reset()

	var l int
	for c, i := range raw {

		if unicode.IsUpper(i) {
			i = unicode.ToLower(i)
		}

		switch {
		case unicode.IsLetter(i):
			if c > 0 && unicode.IsDigit(rune(raw[c-1])) {
				//разделяем цифры и буквы пробелом
				//out.WriteString(" ")
				tmp.WriteString(" ")
				tmp.WriteTo(out)
				tmp.Reset()
			}
			l = utf8.RuneLen(i)

			tmp.WriteRune(i)

		case unicode.IsDigit(i):

			if c > 0 && unicode.IsLetter(rune(raw[c-l])) {
				//out.WriteString(" ")
				tmp.WriteString(" ")
				tmp.WriteTo(out)
				tmp.Reset()
			}

			tmp.WriteRune(i)

		default:

			if !isSkipped(tmp.Bytes()) {

				tmp.WriteString(" ")
				tmp.WriteTo(out)
			}

			tmp.Reset()
		}
	}

	if !isSkipped(tmp.Bytes()) {
		tmp.WriteString(" ")
		tmp.WriteTo(out)
	}
	return out.String()
}

func db_query(buf rankerList, lenWords uint8) []byte {

	var str bytes.Buffer

	str.WriteString("[")

	for i, r := range buf {

		//fmt.Println(r.uniqWords, r.weight)

		if lenWords > r.uniqWords {
			continue
		}
		if i > 0 {
			str.WriteString(",")
		}

		if rubric, ok := rubrics[r.Item.rubId]; ok {

			str.WriteString(`{"cpu":"`)
			str.WriteString(rubric.Cpu)
			str.WriteString(`", "name":"`)
			str.WriteString(rubric.Name)
			str.WriteString(`"}`)
		}
	}
	str.WriteString("]")
	return str.Bytes()

}

func (r *rankerList) Len() int {

	return len(*r)
}

func (r rankerList) Swap(i, j int) {

	r[j], r[i] = r[i], r[j]
}

func (r rankerList) Less(i, j int) bool {

	return r[i].weight > r[j].weight
}

func sphinx2(query string) []byte {

	b_query := query_prepare(query)

	var buf rankerList
	var limit int

	uniq := uniqPool.Get().(map[string]bool)
	ccache := ccachePool.Get().(map[int]*ranker)

	words := strings.Fields(b_query)

	for _, word := range words {

		err := trie.VisitSubtree(patricia.Prefix(word), func(prefix patricia.Prefix, item patricia.Item) error {

			rubItems := item.([]*Complete)
			for _, rubItem := range rubItems {

				//fmt.Println(word, rubItem.repeat, documentPathList[rubItem.hashId])
				hashId := rubItem.rubId
				wk := fmt.Sprintf("%s_%d", word, hashId)

				if v, ok := ccache[hashId]; ok {

					if _, ok = uniq[wk]; !ok {
						v.weight = v.weight * 4
						v.uniqWords += 1
						uniq[wk] = true
					} else {
						v.weight = v.weight + 1
					}

				} else {
					rank := &ranker{hashId, rubItem.repeat, 1, rubItem}
					ccache[hashId] = rank
					uniq[wk] = true
					buf = append(buf, rank)
				}
			}
			return nil
		})

		if err != nil {
			panic(err)
		}
	}

	sort.Sort(&buf)

	if len(buf) < DISPLAYLIMIT {
		limit = len(buf)
	} else {
		limit = DISPLAYLIMIT
	}

	return db_query(buf[:limit], uint8(len(words)))
	//return nil

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

func treeUpdater() {

	c := time.Tick(4 * time.Hour)

	for _ = range c {
		err := initTrie()
		if err != nil {
			fmt.Println(err)
		}
	}
}

func dbOpen() error {

	var err error
	db, err = sql.Open("mysql", fmt.Sprintf("myprom:weronica@unix(%s)/%s?parseTime=true", mysql_socket, mysql_db))
	//fmt.Printf("+++++ %v\n",err)
	return err

}

func init() {

	flag.StringVar(&mysql_socket, "mysql-socket", "/run/mysqld/mysqld.sock", "mysql-socket=/sock/path.sock")
	flag.StringVar(&mysql_db, "mysql-db", "myprom", "mysql-db=mydb")

	uniqPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]bool, 2)
		},
	}
	ccachePool = sync.Pool{
		New: func() interface{} {
			return make(map[int]*ranker, 20)
		},
	}
	tmpPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer([]byte(""))
		},
	}
	outPool = sync.Pool{
		New: func() interface{} {
			return bytes.NewBuffer([]byte(""))
		},
	}

}

func initTrie() error {

	rubrics = make(map[int]rubric, 100)
	trie = patricia.NewTrie()

	rows, err := db.Query("SELECT name,id,cpu FROM rubrics WHERE counter > 0")

	if err != nil {
		return (err)
	}

	defer rows.Close()

	for rows.Next() {

		var (
			name, cpu string
			id        int
		)

		err = rows.Scan(&name, &id, &cpu)

		if err != nil {
			return (err)
		}
		clean_name := query_prepare(name)

		words := strings.Fields(clean_name)

		rubrics[id] = rubric{Cpu: cpu, Name: name}

		cache := make(map[string]*Complete)
		var item *Complete

		for _, word := range words {

			key := word

			if i, ok := cache[word]; ok {
				//				fmt.Println("SKIP", word, "|||", path)
				i.repeat += 1

			} else {

				item = &Complete{id, 1}
				cache[word] = item

				if exists := trie.Get(patricia.Prefix(key)); exists != nil {

					list := exists.([]*Complete)
					list = append(list, item)
					trie.Set(patricia.Prefix(key), list)

				} else {

					items := []*Complete{item}
					trie.Insert(patricia.Prefix(key), items)
				}

			}
		}
	}
	return nil
}

func main() {

	flag.Parse()

	runtime.GOMAXPROCS(2)

	err := dbOpen()
	if err != nil {
		panic(err)
	}

	err = initTrie()

	mux := http.NewServeMux()
	mux.HandleFunc("/completer/", completer)

	go treeUpdater()

	//pid.CreatePid()
	err = Run(mux)
	if err != nil {
		panic(err)
	}

}
