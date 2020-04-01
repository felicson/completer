package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"myprom/completer/storage"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
	"github.com/tchap/go-patricia/patricia"

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
	skippedWords = map[string]struct{}{
		"и":   struct{}{},
		"для": struct{}{},
		"к":   struct{}{},
	}

	ccachePool sync.Pool
	uniqPool   sync.Pool
	tmpPool    sync.Pool
	outPool    sync.Pool
)

type Storage interface {
	GetEntries() ([]storage.Rubric, error)
}

type rubric struct {
	Name string `json:"name"`
	Slug string `json:"cpu"`
}

type Complete struct {
	rubID  int
	repeat uint8
}

var rubrics map[int]rubric

var trie *patricia.Trie //main data structure

type ranker struct {
	rubID     int
	weight    uint8
	uniqWords uint8
	Item      *Complete
}

type rankerList []*ranker

func isSkipped(word []byte) bool {

	if _, ok := skippedWords[string(word)]; ok {

		return ok
	}
	return false
}
func queryPrepare(raw string) string {

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

func makeJSON(buf rankerList, lenWords uint8) []byte {

	var str bytes.Buffer

	str.WriteString(`{"suggestions":[`)

	for i, r := range buf {

		//fmt.Println(r.uniqWords, r.weight)

		if lenWords > r.uniqWords {
			continue
		}
		if i > 0 {
			str.WriteString(",")
		}

		if rubric, ok := rubrics[r.Item.rubID]; ok {

			str.WriteString(`{"value":"`)
			str.WriteString(rubric.Name)
			str.WriteString(`", "data":"`)
			str.WriteString(rubric.Slug)
			str.WriteString(`"}`)
		}
	}
	str.WriteString("]}")
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

func search(query string) ([]byte, error) {

	bQuery := queryPrepare(query)

	var buf rankerList
	var limit int

	uniq := uniqPool.Get().(map[string]bool)
	ccache := ccachePool.Get().(map[int]*ranker)

	words := strings.Fields(bQuery)

	for _, word := range words {

		err := trie.VisitSubtree(patricia.Prefix(word), func(prefix patricia.Prefix, item patricia.Item) error {

			rubItems := item.([]*Complete)
			for _, rubItem := range rubItems {

				//fmt.Println(word, rubItem.repeat, documentPathList[rubItem.hashId])
				hashID := rubItem.rubID
				wk := fmt.Sprintf("%s_%d", word, hashID)

				if v, ok := ccache[hashID]; ok {

					if _, ok = uniq[wk]; !ok {
						v.weight = v.weight * 4
						v.uniqWords++
						uniq[wk] = true
					} else {
						v.weight = v.weight + 1
					}

				} else {
					rank := &ranker{hashID, rubItem.repeat, 1, rubItem}
					ccache[hashID] = rank
					uniq[wk] = true
					buf = append(buf, rank)
				}
			}
			return nil
		})

		if err != nil {
			return nil, err
		}
	}

	sort.Sort(&buf)

	if len(buf) < DISPLAYLIMIT {
		limit = len(buf)
	} else {
		limit = DISPLAYLIMIT
	}

	return makeJSON(buf[:limit], uint8(len(words))), nil
}

func completer(w http.ResponseWriter, req *http.Request) {

	query := req.FormValue("query")

	w.Header().Set("Content-type", "application/json; charset=utf-8")

	if query != "" {
		result, err := search(query)
		if err != nil {
			log.Println(err)
			io.WriteString(w, "[]")
			return
		}
		w.Write(result)
		return
	}
	io.WriteString(w, "[]")
}

//Run start server
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

	server := http.Server{
		Handler: mux,
	}
	sigint := make(chan os.Signal)
	signal.Notify(sigint, syscall.SIGTERM, syscall.SIGINT)

	e := make(chan error)
	go func() {
		e <- server.Serve(ln)
	}()

	select {
	case err := <-e:
		return err
	case <-sigint:
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	log.Print("Starting shutdown server")
	if err := server.Shutdown(ctx); err != nil {
		return err
	}

	return nil

}

func treeUpdater(storage Storage) {

	c := time.Tick(4 * time.Hour)

	for range c {
		err := initTrie(storage)
		if err != nil {
			log.Println(err)
		}
	}
}

func init() {

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

func initTrie(storage Storage) error {

	rubrics = make(map[int]rubric, 100)
	trie = patricia.NewTrie()

	entries, err := storage.GetEntries()
	if err != nil {
		return err
	}
	for _, entry := range entries {

		cleanName := queryPrepare(entry.Name)

		words := strings.Fields(cleanName)

		rubrics[entry.ID] = rubric{Slug: entry.Slug, Name: entry.Name}

		cache := make(map[string]*Complete)
		var item *Complete

		for _, word := range words {

			key := word

			if i, ok := cache[word]; ok {
				i.repeat++

			} else {

				item = &Complete{entry.ID, 1}
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

	runtime.GOMAXPROCS(runtime.NumCPU())

	var mysqlUser, mysqlPass, mysqlDB string
	flag.StringVar(&mysqlUser, "mysql-user", "test", "mysql-user=test")
	flag.StringVar(&mysqlPass, "mysql-password", "test", "mysql-password=test")
	flag.StringVar(&mysqlDB, "mysql-db", "db", "mysql-db=mydb")
	flag.Parse()

	storage, err := storage.NewStorage(mysqlUser, mysqlPass, mysqlDB)
	if err != nil {
		panic(err)
	}

	if err := initTrie(storage); err != nil {
		panic(err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/completer/", completer)

	go treeUpdater(storage)

	err = Run(mux)
	if err != nil {
		panic(err)
	}

}
